import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz
import jellyfish
from Levenshtein import distance as levenshtein_distance
import re
from typing import Tuple, Optional, Dict, List
from app.db.database import execute_query
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import multiprocessing
import os
import sys


def _process_batch_chunk(batch_data: Dict) -> Dict:
    """
    处理单个批次的匹配（用于多进程）
    batch_data: {
        'import_rows': List[Dict],  # 待匹配的行数据
        'db_df_dict': Dict,  # 数据库DataFrame的字典表示
        'exact_match_dict': Dict,
        'genus_index': Dict,
        'phonetic_dict': Dict
    }
    """
    import_rows = batch_data['import_rows']
    db_df = pd.DataFrame(batch_data['db_df_dict'])
    exact_match_dict = batch_data['exact_match_dict']
    genus_index = batch_data['genus_index']
    phonetic_dict = batch_data['phonetic_dict']

    batch_matches = {
        "exact": [],
        "fuzzy": [],
        "spelling_error": [],
        "phonetic": [],
        "no_match": []
    }

    for row_dict in import_rows:
        idx = row_dict['index']
        normalized_full_name = row_dict.get('normalized_full_name', '')
        normalized_genus = row_dict.get('normalized_genus', '')
        normalized_species = row_dict.get('normalized_species', '')
        original_full_name = row_dict.get('original_full_name', '')
        genus_soundex = row_dict.get('genus_soundex', '')
        species_soundex = row_dict.get('species_soundex', '')

        if not normalized_full_name or not normalized_full_name.strip():
            batch_matches["no_match"].append({
                "import_index": idx,
                "original_name": original_full_name,
                "reason": "invalid or null name",
                "match_status": "no_match"
            })
            continue

        # 1. 完全匹配
        if normalized_full_name in exact_match_dict:
            db_idx = exact_match_dict[normalized_full_name]
            batch_matches["exact"].append({
                "import_index": idx,
                "taxon_id": db_df.loc[db_idx, 'TaxonID'],
                "import_name": original_full_name,
                "db_name": db_df.loc[db_idx, 'original_full_name'],
                "similarity": 100,
                "import_normalized": normalized_full_name,
                "db_normalized": db_df.loc[db_idx, 'normalized_full_name'],
                "match_status": "exact"
            })
            continue

        # 2. 模糊匹配和拼写错误匹配
        best_fuzzy_similarity = 0
        best_fuzzy_match = None
        best_spelling_distance = 999
        best_spelling_match = None

        candidate_indices = set()

        if normalized_genus in genus_index:
            candidate_indices.update(genus_index[normalized_genus])

        # 只在没有完全相同属名时，才查找相近属名（限制搜索量）
        if not candidate_indices and normalized_genus:
            # 优化：只检查前缀相似或长度相近的属名，大大减少比较次数
            genus_len = len(normalized_genus)
            for genus_key in genus_index.keys():
                if genus_key:
                    # 快速过滤：长度差异>2的直接跳过
                    if abs(len(genus_key) - genus_len) > 2:
                        continue
                    # 快速过滤：首字母不同的直接跳过（属名首字母很重要）
                    if genus_key[0] != normalized_genus[0]:
                        continue
                    # 才进行编辑距离计算
                    genus_distance = levenshtein_distance(normalized_genus, genus_key)
                    if genus_distance <= 2:
                        candidate_indices.update(genus_index[genus_key])
                        # 找到一些候选后就停止（避免过多候选）
                        if len(candidate_indices) > 100:
                            break

        # 限制候选集大小，避免对太多记录进行模糊匹配
        if len(candidate_indices) > 200:
            # 如果候选太多，只取前200个（按TaxonID排序保证稳定性）
            candidate_indices = set(sorted(candidate_indices)[:200])

        for db_idx in candidate_indices:
            db_name = db_df.loc[db_idx, 'normalized_full_name']

            similarity = fuzz.ratio(normalized_full_name, db_name)
            if similarity > 90 and similarity > best_fuzzy_similarity:
                best_fuzzy_similarity = similarity
                best_fuzzy_match = db_idx

            name_distance = levenshtein_distance(normalized_full_name, db_name)
            if name_distance <= 3 and name_distance < best_spelling_distance:
                best_spelling_distance = name_distance
                best_spelling_match = db_idx

        if best_fuzzy_match is not None:
            batch_matches["fuzzy"].append({
                "import_index": idx,
                "taxon_id": db_df.loc[best_fuzzy_match, 'TaxonID'],
                "import_name": original_full_name,
                "db_name": db_df.loc[best_fuzzy_match, 'original_full_name'],
                "similarity": best_fuzzy_similarity,
                "import_normalized": normalized_full_name,
                "db_normalized": db_df.loc[best_fuzzy_match, 'normalized_full_name'],
                "match_status": "fuzzy"
            })
            continue

        if best_spelling_match is not None:
            batch_matches["spelling_error"].append({
                "import_index": idx,
                "taxon_id": db_df.loc[best_spelling_match, 'TaxonID'],
                "import_name": original_full_name,
                "db_name": db_df.loc[best_spelling_match, 'original_full_name'],
                "edit_distance": best_spelling_distance,
                "import_normalized": normalized_full_name,
                "db_normalized": db_df.loc[best_spelling_match, 'normalized_full_name'],
                "match_status": "spelling_error"
            })
            continue

        # 3. 语音编码匹配
        if normalized_genus and normalized_species:
            phonetic_key = f"{genus_soundex}_{species_soundex}"

            if phonetic_key in phonetic_dict:
                phonetic_matches = []
                for db_idx in phonetic_dict[phonetic_key][:5]:
                    phonetic_matches.append({
                        "taxon_id": db_df.loc[db_idx, 'TaxonID'],
                        "db_name": db_df.loc[db_idx, 'original_full_name'],
                        "db_normalized": db_df.loc[db_idx, 'normalized_full_name']
                    })

                batch_matches["phonetic"].append({
                    "import_index": idx,
                    "import_name": original_full_name,
                    "import_normalized": normalized_full_name,
                    "potential_matches": phonetic_matches,
                    "soundex_key": phonetic_key,
                    "match_status": "phonetic"
                })
                continue

        # 4. 完全不匹配
        batch_matches["no_match"].append({
            "import_index": idx,
            "original_name": original_full_name,
            "normalized_name": normalized_full_name,
            "reason": "match not found",
            "match_status": "no_match"
        })

    return batch_matches


async def validate_scientific_name(name: str) -> Tuple[bool, Optional[str], Optional[str], Optional[str]]:
    """
    验证科学名称格式
    返回: (是否有效, 属名, 种名, 亚种名)
    """
    if not name or pd.isna(name):
        return False, None, None, None

    name_str = str(name).strip()

    # 基本的属种格式
    genus_species_pattern = r'^([A-Z][a-z]+)\s+([a-z\-]+)$'

    # 亚种格式
    subspecies_pattern = r'^([A-Z][a-z]+)\s+([a-z\-]+)\s+([a-z\-]+)$'

    genus_match = re.match(genus_species_pattern, name_str)
    if genus_match:
        genus = genus_match.group(1)
        species = genus_match.group(2)
        subspecies = None
        return True, genus, species, subspecies

    subspecies_match = re.match(subspecies_pattern, name_str)
    if subspecies_match:
        genus = subspecies_match.group(1)
        species = subspecies_match.group(2)
        subspecies = subspecies_match.group(3)
        return True, genus, species, subspecies

    return False, None, None, None


class SpeciesNameValidator:
    """物种名称验证和匹配工具类"""

    def __init__(self):
        self.authority_dict = self._load_authority_dict()
        self.taxonomic_markers = self._load_taxonomic_markers()

    def _load_authority_dict(self) -> Dict[str, str]:
        """加载权威人名缩写字典"""
        return {
            "L.": "Linnaeus",
            "Lam.": "Lamarck",
            "DC.": "De Candolle",
            "Willd.": "Willdenow",
            "Benth.": "Bentham",
            "Hook.": "Hooker",
            "F.Muell.": "Ferdinand Mueller",
            "Mill.": "Miller",
            "Blume": "Blume",
            "A.Gray": "Asa Gray",
            "Sm.": "Smith",
            "Thunb.": "Thunberg",
            "Wall.": "Wallich",
            "Roxb.": "Roxburgh",
            "R.Br.": "Robert Brown",
            "Griseb.": "Grisebach",
            "Sw.": "Swartz",
            "A.Juss.": "Adrien de Jussieu"
        }

    def _load_taxonomic_markers(self) -> List[str]:
        """加载分类学标记"""
        return [
            "sp. nov.", "sp.nov.", "sp nov", "species nova", "species novum",
            "gen. nov.", "gen.nov.", "gen nov", "genus novum",
            "comb. nov.", "comb.nov.", "comb nov", "combinatio nova",
            "stat. nov.", "stat.nov.", "stat nov", "status novus",
            "nom. nov.", "nom.nov.", "nom nov", "nomen novum",
            "nom. cons.", "nom.cons.", "nomen conservandum",
            "nom. rej.", "nom.rej.", "nomen rejiciendum",
            "nom. illeg.", "nom.illeg.", "nomen illegitimum",
            "nom. inval.", "nom.inval.", "nomen invalidum",
            "nom. dub.", "nom.dub.", "nomen dubium",
            "nom. nud.", "nom.nud.", "nomen nudum",
            "s.s.", "s.l.", "sensu stricto", "sensu lato",
            "cf.", "aff.", "sp.", "spp.", "ssp.", "subsp.", "var.", "f.", "forma"
        ]

    def normalize_taxon_name(self, name: str) -> str:
        """
        标准化分类群名称
        1. 转换为小写
        2. 移除分类学标记和变种标记
        3. 移除括号和引号
        4. 规范化空格
        """
        if pd.isna(name) or not name:
            return ""

        # 转换为小写
        name = str(name).strip().lower()

        # 移除分类学标记
        for marker in self.taxonomic_markers:
            name = name.replace(marker.lower(), " ")

        # 移除常见的分类学附加信息
        patterns = [
            r'\bvar\.\s+\S+',
            r'\bsubsp\.\s+\S+',
            r'\bssp\.\s+\S+',
            r'\bf\.\s+\S+',
            r'\bforma\s+\S+',
            r'\bsp\.\s*$',
            r'\bsp\s*$',
            r'\bspp\.\s*$',
            r'\baff\.\s+\S+',
            r'\bcf\.\s+\S+',
        ]

        for pattern in patterns:
            name = re.sub(pattern, '', name)

        # 移除括号、引号和内容
        name = re.sub(r'\([^)]*\)', '', name)
        name = re.sub(r'\[[^\]]*\]', '', name)
        name = re.sub(r'"[^"]*"', '', name)
        name = re.sub(r"'[^']*'", '', name)

        # 处理连字符和下划线
        name = name.replace('-', ' ')
        name = name.replace('_', ' ')

        # 规范化空格
        name = re.sub(r'\s+', ' ', name).strip()

        return name

    def process_authority(self, taxon_name: str) -> str:
        """处理学名中的权威人名缩写"""
        if pd.isna(taxon_name) or not taxon_name:
            return ""

        processed_name = str(taxon_name)

        # 检查是否包含权威人名缩写
        for abbrev, full_name in self.authority_dict.items():
            pattern = r'\b' + re.escape(abbrev) + r'\b'
            if re.search(pattern, processed_name):
                processed_name = re.sub(pattern, full_name, processed_name)

        return processed_name

    # 缓存数据库分类数据
    _cached_db_data = None
    _cache_timestamp = None
    _cache_ttl = 3600  # 缓存1小时

    async def _get_cached_taxonomic_data(self):
        """获取缓存的分类数据，如果缓存过期则重新加载"""
        import time

        current_time = time.time()
        if (self._cached_db_data is None or
            self._cache_timestamp is None or
            current_time - self._cache_timestamp > self._cache_ttl):

            # 重新加载数据
            db_query = """
            SELECT "TaxonID", "FamilyName", "Genus", "Species"
            FROM taxonomic_table
            """

            db_taxonomic = await execute_query(db_query)
            if not db_taxonomic:
                return None

            # 转换为DataFrame并预处理
            db_df = pd.DataFrame(db_taxonomic)

            # 预处理数据库数据
            db_df['original_genus'] = db_df['Genus'].fillna('')
            db_df['original_species'] = db_df['Species'].fillna('')
            db_df['original_full_name'] = db_df['original_genus'] + ' ' + db_df['original_species']

            db_df['processed_genus'] = db_df['Genus'].apply(
                lambda x: self.process_authority(x))
            db_df['processed_species'] = db_df['Species'].apply(
                lambda x: self.process_authority(x))

            db_df['normalized_genus'] = db_df['processed_genus'].apply(
                lambda x: self.normalize_taxon_name(x))
            db_df['normalized_species'] = db_df['processed_species'].apply(
                lambda x: self.normalize_taxon_name(x))

            db_df['normalized_full_name'] = (
                db_df['normalized_genus'] + ' ' + db_df['normalized_species']
            ).str.strip()

            # 建立多种索引以提高匹配效率
            # 1. 完全匹配索引
            exact_match_dict = {}
            # 2. 属名索引
            genus_index = {}
            # 3. 语音编码索引
            phonetic_dict = {}

            for idx, row in db_df.iterrows():
                if pd.notna(row['normalized_full_name']) and row['normalized_full_name'].strip():
                    # 完全匹配索引
                    normalized_name = row['normalized_full_name']
                    exact_match_dict[normalized_name] = idx

                    # 按属名分组索引（用于优化模糊匹配和拼写错误匹配）
                    genus = row['normalized_genus']
                    if genus and genus.strip():
                        if genus not in genus_index:
                            genus_index[genus] = []
                        genus_index[genus].append(idx)

                    # 创建语音编码索引
                    genus_sound = jellyfish.soundex(row['normalized_genus']) if pd.notna(row['normalized_genus']) and row['normalized_genus'] else ""
                    species_sound = jellyfish.soundex(row['normalized_species']) if pd.notna(row['normalized_species']) and row['normalized_species'] else ""
                    if genus_sound or species_sound:
                        phonetic_key = f"{genus_sound}_{species_sound}"
                        if phonetic_key not in phonetic_dict:
                            phonetic_dict[phonetic_key] = []
                        phonetic_dict[phonetic_key].append(idx)

            # 缓存数据
            self._cached_db_data = {
                'df': db_df,
                'exact_match_dict': exact_match_dict,
                'genus_index': genus_index,
                'phonetic_dict': phonetic_dict
            }
            self._cache_timestamp = current_time

        return self._cached_db_data

    async def batch_match_taxonomic_names(self, import_data: List[Dict]) -> Dict:
        """
        批量匹配分类群名称，优化版本
        import_data: 包含family, genus, species的字典列表
        """
        import time
        import logging
        import os

        # 配置日志
        logger = logging.getLogger('species_validation')
        if not logger.handlers:
            # 确保logs目录存在
            os.makedirs('logs', exist_ok=True)
            handler = logging.FileHandler('logs/species_matching.log', encoding='utf-8')
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)

        start_time = time.time()

        print(f"\n{'='*60}")
        print(f"Starting batch taxonomic name matching - Total records: {len(import_data)}")
        print(f"{'='*60}")
        logger.info(f"Starting batch taxonomic name matching - Total records: {len(import_data)}")

        # 获取缓存的数据库数据
        cache_start = time.time()
        print(f"[1/5] Loading database taxonomic data...")
        logger.info("[1/5] Loading database taxonomic data...")
        cached_data = await self._get_cached_taxonomic_data()
        if not cached_data:
            error_msg = "No taxonomic data found in database"
            logger.error(error_msg)
            return {"error": error_msg}

        db_df = cached_data['df']
        exact_match_dict = cached_data['exact_match_dict']
        genus_index = cached_data['genus_index']
        phonetic_dict = cached_data['phonetic_dict']

        cache_time = time.time() - cache_start
        print(f"      ✓ Database records: {len(db_df)}")
        print(f"      ✓ Genus index: {len(genus_index)} unique genera")
        print(f"      ✓ Time elapsed: {cache_time:.2f}s")
        logger.info(f"Database loaded - Records: {len(db_df)}, Genera: {len(genus_index)}, Time: {cache_time:.2f}s")

        # 转换导入数据为DataFrame
        preprocess_start = time.time()
        print(f"\n[2/5] Preprocessing import data...")
        logger.info("[2/5] Preprocessing import data...")
        import_df = pd.DataFrame(import_data)

        # 预处理导入数据 - 使用向量化操作提速
        import_df['original_genus'] = import_df['genus'].fillna('')
        import_df['original_species'] = import_df['species'].fillna('')
        import_df['original_full_name'] = import_df['original_genus'] + ' ' + import_df['original_species']

        # 快速向量化的标准化处理（跳过复杂的process_authority，直接normalize）
        # 对于大多数情况，权威人名不影响匹配结果
        import_df['normalized_genus'] = (
            import_df['genus']
            .fillna('')
            .str.strip()
            .str.lower()
            # 移除括号和引号内容
            .str.replace(r'\([^)]*\)', '', regex=True)
            .str.replace(r'\[[^\]]*\]', '', regex=True)
            .str.replace(r'"[^"]*"', '', regex=True)
            .str.replace(r"'[^']*'", '', regex=True)
            # 处理连字符和下划线
            .str.replace('-', ' ', regex=False)
            .str.replace('_', ' ', regex=False)
            # 规范化空格
            .str.replace(r'\s+', ' ', regex=True)
            .str.strip()
        )

        import_df['normalized_species'] = (
            import_df['species']
            .fillna('')
            .str.strip()
            .str.lower()
            # 移除常见的分类学标记
            .str.replace(r'\bsp\.?\s*$', '', regex=True)
            .str.replace(r'\bspp\.?\s*$', '', regex=True)
            .str.replace(r'\bvar\.\s+\S+', '', regex=True)
            .str.replace(r'\bsubsp\.\s+\S+', '', regex=True)
            # 移除括号和引号内容
            .str.replace(r'\([^)]*\)', '', regex=True)
            .str.replace(r'\[[^\]]*\]', '', regex=True)
            # 处理连字符和下划线
            .str.replace('-', ' ', regex=False)
            .str.replace('_', ' ', regex=False)
            # 规范化空格
            .str.replace(r'\s+', ' ', regex=True)
            .str.strip()
        )

        import_df['normalized_full_name'] = (
            import_df['normalized_genus'] + ' ' + import_df['normalized_species']
        ).str.strip()

        preprocess_time = time.time() - preprocess_start
        print(f"      ✓ Time elapsed: {preprocess_time:.2f}s")
        logger.info(f"Preprocessing completed - Time: {preprocess_time:.2f}s")

        # 匹配结果存储
        matches = {
            "exact": [],
            "fuzzy": [],
            "spelling_error": [],
            "phonetic": [],
            "no_match": []
        }

        # 预缓存 soundex 编码 - 向量化计算
        print(f"\n[3/5] Pre-computing soundex encoding...")
        logger.info("[3/5] Pre-computing soundex encoding...")
        soundex_start = time.time()

        # 使用向量化操作：只对非空值计算soundex
        def safe_soundex(series):
            """向量化的 soundex 计算"""
            result = pd.Series([''] * len(series), index=series.index)
            non_empty = series[series.str.len() > 0]
            if len(non_empty) > 0:
                result.loc[non_empty.index] = non_empty.apply(jellyfish.soundex)
            return result

        import_df['genus_soundex'] = safe_soundex(import_df['normalized_genus'])
        import_df['species_soundex'] = safe_soundex(import_df['normalized_species'])

        soundex_time = time.time() - soundex_start
        print(f"      ✓ Time elapsed: {soundex_time:.2f}s")
        logger.info(f"Soundex encoding completed - Time: {soundex_time:.2f}s")

        # 遍历导入数据进行匹配
        match_start = time.time()
        print(f"\n[4/5] Executing taxonomic matching...")
        logger.info("[4/5] Executing taxonomic matching...")
        total_records = len(import_df)

        # 根据数据量决定是否使用并行处理（超过5000条才使用）
        use_parallel = total_records >= 5000

        # Windows系统使用线程池（避免multiprocessing的pickle问题），Unix系统可以用进程池
        use_threads = sys.platform == 'win32'

        if use_parallel:
            # 获取CPU核心数，限制最大worker数避免过多开销
            cpu_count = multiprocessing.cpu_count() if not use_threads else os.cpu_count() or 4
            num_workers = min(8, max(2, cpu_count - 1))  # 最多8个worker，最少2个
            # 增大批次大小以减少线程通信开销，线程池适合更大批次
            chunk_size = max(5000, total_records // num_workers) if use_threads else max(2000, total_records // (num_workers * 2))
            num_chunks = (total_records + chunk_size - 1) // chunk_size

            worker_type = "threads" if use_threads else "processes"
            print(f"      Using parallel {worker_type}: {num_workers} {worker_type}, {num_chunks} batches")
            logger.info(f"Using parallel {worker_type}: {num_workers} {worker_type}, {num_chunks} batches")

            try:
                # 准备数据库数据的字典表示（用于序列化）
                db_df_dict = db_df.to_dict('list')

                # 准备批次数据
                import_rows_list = []
                for row in import_df.itertuples():
                    import_rows_list.append({
                        'index': row.Index,
                        'normalized_full_name': row.normalized_full_name,
                        'normalized_genus': row.normalized_genus,
                        'normalized_species': row.normalized_species,
                        'original_full_name': row.original_full_name,
                        'genus_soundex': row.genus_soundex,
                        'species_soundex': row.species_soundex
                    })

                # 分割成多个批次
                batch_tasks = []
                for i in range(0, total_records, chunk_size):
                    batch_rows = import_rows_list[i:i + chunk_size]
                    batch_tasks.append({
                        'import_rows': batch_rows,
                        'db_df_dict': db_df_dict,
                        'exact_match_dict': exact_match_dict,
                        'genus_index': genus_index,
                        'phonetic_dict': phonetic_dict
                    })

                # 选择合适的Executor
                ExecutorClass = ThreadPoolExecutor if use_threads else ProcessPoolExecutor
                completed_chunks = 0

                with ExecutorClass(max_workers=num_workers) as executor:
                    futures = {executor.submit(_process_batch_chunk, batch): i
                              for i, batch in enumerate(batch_tasks)}

                    for future in as_completed(futures):
                        try:
                            batch_result = future.result(timeout=300)  # 5分钟超时

                            # 合并结果
                            for match_type in matches.keys():
                                matches[match_type].extend(batch_result[match_type])

                            # 显示进度
                            completed_chunks += 1
                            progress_pct = completed_chunks / num_chunks * 100
                            elapsed = time.time() - match_start
                            eta = elapsed / completed_chunks * (num_chunks - completed_chunks) if completed_chunks > 0 else 0
                            print(f"      Progress: {completed_chunks}/{num_chunks} batches ({progress_pct:.1f}%) - "
                                  f"Elapsed: {elapsed:.1f}s - ETA: {eta:.1f}s", end='\r')
                        except Exception as e:
                            error_msg = f"Batch processing failed, falling back to single thread: {str(e)}"
                            print(f"\n      Warning: {error_msg}")
                            logger.warning(error_msg)
                            # 并行失败，回退到单线程
                            use_parallel = False
                            matches = {
                                "exact": [],
                                "fuzzy": [],
                                "spelling_error": [],
                                "phonetic": [],
                                "no_match": []
                            }
                            break

            except Exception as e:
                error_msg = f"Parallel processing initialization failed, using single thread: {str(e)}"
                print(f"\n      Warning: {error_msg}")
                logger.warning(error_msg)
                use_parallel = False
                matches = {
                    "exact": [],
                    "fuzzy": [],
                    "spelling_error": [],
                    "phonetic": [],
                    "no_match": []
                }

        if not use_parallel:
            # 单线程处理（小数据集或并行失败时的回退）
            if total_records >= 5000:
                msg = "Using single thread processing (parallel processing failed, fallback)"
                print(f"      {msg}")
                logger.info(msg)
            else:
                msg = "Using single thread processing (small dataset)"
                print(f"      {msg}")
                logger.info(msg)

            progress_interval = max(1, total_records // 20)

            for row in import_df.itertuples():
                idx = row.Index

                # 显示进度
                if idx % progress_interval == 0 or idx == total_records - 1:
                    progress_pct = (idx + 1) / total_records * 100
                    elapsed = time.time() - match_start
                    if idx > 0:
                        eta = elapsed / (idx + 1) * (total_records - idx - 1)
                        print(f"      Progress: {idx + 1}/{total_records} ({progress_pct:.1f}%) - "
                              f"Elapsed: {elapsed:.1f}s - ETA: {eta:.1f}s", end='\r')

                if pd.isna(row.normalized_full_name) or not row.normalized_full_name.strip():
                    matches["no_match"].append({
                        "import_index": idx,
                        "original_name": getattr(row, 'original_full_name', ''),
                        "reason": "invalid or null name",
                        "match_status": "no_match"
                    })
                    continue

                normalized_full_name = row.normalized_full_name
                normalized_genus = row.normalized_genus
                normalized_species = row.normalized_species
                original_full_name = row.original_full_name

                # 1. 完全匹配（O(1) 查找）
                if normalized_full_name in exact_match_dict:
                    db_idx = exact_match_dict[normalized_full_name]
                    matches["exact"].append({
                        "import_index": idx,
                        "taxon_id": db_df.loc[db_idx, 'TaxonID'],
                        "import_name": original_full_name,
                        "db_name": db_df.loc[db_idx, 'original_full_name'],
                        "similarity": 100,
                        "import_normalized": normalized_full_name,
                        "db_normalized": db_df.loc[db_idx, 'normalized_full_name'],
                        "match_status": "exact"
                    })
                    continue

                # 2. 模糊匹配和拼写错误匹配 - 仅在相同属名或相近属名的记录中查找
                best_fuzzy_similarity = 0
                best_fuzzy_match = None
                best_spelling_distance = 999
                best_spelling_match = None

                # 获取候选记录：相同属名 + 编辑距离<=2的相近属名
                candidate_indices = set()

                # 添加完全相同属名的记录
                if normalized_genus in genus_index:
                    candidate_indices.update(genus_index[normalized_genus])

                # 添加编辑距离<=2的相近属名记录（仅对拼写错误匹配）
                # 只在没有完全相同属名匹配时才执行（Early stopping优化）
                if not candidate_indices and normalized_genus:
                    # 优化：只检查前缀相似或长度相近的属名
                    genus_len = len(normalized_genus)
                    for genus_key in genus_index.keys():
                        if genus_key:
                            # 快速过滤：长度差异>2的直接跳过
                            if abs(len(genus_key) - genus_len) > 2:
                                continue
                            # 快速过滤：首字母不同的直接跳过
                            if genus_key[0] != normalized_genus[0]:
                                continue
                            # 才进行编辑距离计算
                            genus_distance = levenshtein_distance(normalized_genus, genus_key)
                            if genus_distance <= 2:
                                candidate_indices.update(genus_index[genus_key])
                                # 找到一些候选后就停止
                                if len(candidate_indices) > 100:
                                    break

                # 限制候选集大小
                if len(candidate_indices) > 200:
                    candidate_indices = set(sorted(candidate_indices)[:200])

                # 在候选记录中进行匹配
                for db_idx in candidate_indices:
                    db_name = db_df.loc[db_idx, 'normalized_full_name']

                    # 模糊匹配
                    similarity = fuzz.ratio(normalized_full_name, db_name)
                    if similarity > 90 and similarity > best_fuzzy_similarity:
                        best_fuzzy_similarity = similarity
                        best_fuzzy_match = db_idx

                    # 拼写错误匹配
                    name_distance = levenshtein_distance(normalized_full_name, db_name)
                    if name_distance <= 3 and name_distance < best_spelling_distance:
                        best_spelling_distance = name_distance
                        best_spelling_match = db_idx

                # 优先返回模糊匹配结果
                if best_fuzzy_match is not None:
                    matches["fuzzy"].append({
                        "import_index": idx,
                        "taxon_id": db_df.loc[best_fuzzy_match, 'TaxonID'],
                        "import_name": original_full_name,
                        "db_name": db_df.loc[best_fuzzy_match, 'original_full_name'],
                        "similarity": best_fuzzy_similarity,
                        "import_normalized": normalized_full_name,
                        "db_normalized": db_df.loc[best_fuzzy_match, 'normalized_full_name'],
                        "match_status": "fuzzy"
                    })
                    continue

                # 其次返回拼写错误匹配结果
                if best_spelling_match is not None:
                    matches["spelling_error"].append({
                        "import_index": idx,
                        "taxon_id": db_df.loc[best_spelling_match, 'TaxonID'],
                        "import_name": original_full_name,
                        "db_name": db_df.loc[best_spelling_match, 'original_full_name'],
                        "edit_distance": best_spelling_distance,
                        "import_normalized": normalized_full_name,
                        "db_normalized": db_df.loc[best_spelling_match, 'normalized_full_name'],
                        "match_status": "spelling_error"
                    })
                    continue

                # 3. 语音编码匹配（使用预缓存的soundex）
                if normalized_genus and normalized_species:
                    genus_sound = row.genus_soundex
                    species_sound = row.species_soundex
                    phonetic_key = f"{genus_sound}_{species_sound}"

                    if phonetic_key in phonetic_dict:
                        phonetic_matches = []
                        for db_idx in phonetic_dict[phonetic_key][:5]:  # 限制为最多5个语音匹配
                            phonetic_matches.append({
                                "taxon_id": db_df.loc[db_idx, 'TaxonID'],
                                "db_name": db_df.loc[db_idx, 'original_full_name'],
                                "db_normalized": db_df.loc[db_idx, 'normalized_full_name']
                            })

                        matches["phonetic"].append({
                            "import_index": idx,
                            "import_name": original_full_name,
                            "import_normalized": normalized_full_name,
                            "potential_matches": phonetic_matches,
                            "soundex_key": phonetic_key,
                            "match_status": "phonetic"
                        })
                        continue

                # 4. 完全不匹配
                matches["no_match"].append({
                    "import_index": idx,
                    "original_name": original_full_name,
                    "normalized_name": normalized_full_name,
                    "reason": "match not found",
                    "match_status": "no_match"
                })

        match_time = time.time() - match_start
        print(f"\n      ✓ Time elapsed: {match_time:.2f}s")
        logger.info(f"Matching completed - Time: {match_time:.2f}s")

        # 生成匹配统计报告
        summary_start = time.time()
        print(f"\n[5/5] Generating match statistics...")
        logger.info("[5/5] Generating match statistics...")
        total_records = len(import_df)
        exact_count = len(matches["exact"])
        fuzzy_count = len(matches["fuzzy"])
        spelling_count = len(matches["spelling_error"])
        phonetic_count = len(matches["phonetic"])
        no_match_count = len(matches["no_match"])

        match_summary = {
            "total_records": total_records,
            "exact_matches": exact_count,
            "fuzzy_matches": fuzzy_count,
            "spelling_error_matches": spelling_count,
            "phonetic_matches": phonetic_count,
            "no_matches": no_match_count,
            "match_rate": round((exact_count + fuzzy_count + spelling_count) / total_records * 100,
                                2) if total_records > 0 else 0
        }

        summary_time = time.time() - summary_start
        total_time = time.time() - start_time

        print(f"      ✓ Time elapsed: {summary_time:.2f}s")
        print(f"\n{'='*60}")
        print(f"Matching Completed Summary:")
        print(f"  • Total records: {total_records}")
        print(f"  • Exact matches: {exact_count} ({exact_count/total_records*100:.1f}%)" if total_records > 0 else "  • Exact matches: 0")
        print(f"  • Fuzzy matches: {fuzzy_count} ({fuzzy_count/total_records*100:.1f}%)" if total_records > 0 else "  • Fuzzy matches: 0")
        print(f"  • Spelling errors: {spelling_count} ({spelling_count/total_records*100:.1f}%)" if total_records > 0 else "  • Spelling errors: 0")
        print(f"  • Phonetic matches: {phonetic_count} ({phonetic_count/total_records*100:.1f}%)" if total_records > 0 else "  • Phonetic matches: 0")
        print(f"  • No matches: {no_match_count} ({no_match_count/total_records*100:.1f}%)" if total_records > 0 else "  • No matches: 0")
        print(f"  • Match success rate: {match_summary['match_rate']}%")
        print(f"\nPerformance Statistics:")
        print(f"  • Data loading: {cache_time:.2f}s")
        print(f"  • Data preprocessing: {preprocess_time:.2f}s")
        print(f"  • Soundex pre-computation: {soundex_time:.2f}s")
        print(f"  • Matching processing: {match_time:.2f}s (avg {match_time/total_records*1000:.2f}ms/record)" if total_records > 0 else f"  • Matching processing: {match_time:.2f}s")
        print(f"  • Statistics summary: {summary_time:.2f}s")
        print(f"  • Total time: {total_time:.2f}s")
        print(f"{'='*60}\n")

        # 记录完整的统计信息到日志
        logger.info(f"Matching completed - Total: {total_records}, Exact: {exact_count}, Fuzzy: {fuzzy_count}, "
                   f"Spelling: {spelling_count}, Phonetic: {phonetic_count}, No match: {no_match_count}")
        logger.info(f"Performance - Load: {cache_time:.2f}s, Preprocess: {preprocess_time:.2f}s, "
                   f"Soundex: {soundex_time:.2f}s, Match: {match_time:.2f}s, Total: {total_time:.2f}s")
        logger.info(f"Match success rate: {match_summary['match_rate']}%")

        return {
            "matches": matches,
            "summary": match_summary
        }

    async def find_single_taxon_match(self, family: str, genus: str, species: str) -> Dict:
        """
        为单个分类群查找匹配
        """
        result = await self.batch_match_taxonomic_names([{
            "family": family,
            "genus": genus,
            "species": species
        }])

        if "error" in result:
            return {
                "matched": False,
                "taxon_id": None,
                "match_type": "error",
                "error": result["error"]
            }

        matches = result["matches"]

        # 按优先级返回匹配结果
        if matches["exact"]:
            match = matches["exact"][0]
            return {
                "matched": True,
                "taxon_id": match["taxon_id"],
                "match_type": "exact",
                "matched_name": match["db_name"],
                "confidence": match["similarity"]
            }
        elif matches["fuzzy"]:
            match = matches["fuzzy"][0]
            return {
                "matched": True,
                "taxon_id": match["taxon_id"],
                "match_type": "fuzzy",
                "matched_name": match["db_name"],
                "confidence": match["similarity"]
            }
        elif matches["spelling_error"]:
            match = matches["spelling_error"][0]
            return {
                "matched": True,
                "taxon_id": match["taxon_id"],
                "match_type": "spelling_error",
                "matched_name": match["db_name"],
                "confidence": max(0, 100 - (match["edit_distance"] * 20))
            }
        elif matches["phonetic"]:
            match = matches["phonetic"][0]
            if match["potential_matches"]:
                potential = match["potential_matches"][0]
                return {
                    "matched": True,
                    "taxon_id": potential["taxon_id"],
                    "match_type": "phonetic",
                    "matched_name": potential["db_name"],
                    "confidence": 60  # 语音匹配置信度较低
                }

        return {
            "matched": False,
            "taxon_id": None,
            "match_type": "no_match",
            "matched_name": None,
            "confidence": 0
        }