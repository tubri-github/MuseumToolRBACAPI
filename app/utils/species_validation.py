import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz
import jellyfish
from Levenshtein import distance as levenshtein_distance
import re
from typing import Tuple, Optional, Dict, List
from app.db.database import execute_query


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

    async def batch_match_taxonomic_names(self, import_data: List[Dict]) -> Dict:
        """
        批量匹配分类群名称，基于原始代码的TAXAMATCH功能
        import_data: 包含family, genus, species的字典列表
        """
        # 获取数据库中的所有分类群数据
        db_query = """
        SELECT "TaxonID", "FamilyName", "Genus", "Species"
        FROM taxonomic_table
        """

        db_taxonomic = await execute_query(db_query)
        if not db_taxonomic:
            return {"error": "No taxonomic data found in database"}

        # 转换为DataFrame便于处理
        db_df = pd.DataFrame(db_taxonomic)
        import_df = pd.DataFrame(import_data)

        # 预处理数据库数据
        db_df['original_genus'] = db_df['Genus'].copy()
        db_df['original_species'] = db_df['Species'].copy()
        db_df['original_full_name'] = db_df['original_genus'] + ' ' + db_df['original_species']

        db_df['processed_genus'] = db_df['Genus'].apply(
            lambda x: self.process_authority(x))
        db_df['processed_species'] = db_df['Species'].apply(
            lambda x: self.process_authority(x))

        db_df['normalized_genus'] = db_df['processed_genus'].apply(
            lambda x: self.normalize_taxon_name(x))
        db_df['normalized_species'] = db_df['processed_species'].apply(
            lambda x: self.normalize_taxon_name(x))

        db_df['normalized_full_name'] = db_df.apply(
            lambda row: (row['normalized_genus'] + ' ' + row['normalized_species']).strip(), axis=1)

        # 预处理导入数据
        import_df['original_genus'] = import_df['genus'].copy()
        import_df['original_species'] = import_df['species'].copy()
        import_df['original_full_name'] = import_df['original_genus'] + ' ' + import_df['original_species']

        import_df['processed_genus'] = import_df['genus'].apply(
            lambda x: self.process_authority(x))
        import_df['processed_species'] = import_df['species'].apply(
            lambda x: self.process_authority(x))

        import_df['normalized_genus'] = import_df['processed_genus'].apply(
            lambda x: self.normalize_taxon_name(x))
        import_df['normalized_species'] = import_df['processed_species'].apply(
            lambda x: self.normalize_taxon_name(x))

        import_df['normalized_full_name'] = import_df.apply(
            lambda row: (row['normalized_genus'] + ' ' + row['normalized_species']).strip(), axis=1)

        # 建立索引以提高匹配效率
        db_dict = {}
        phonetic_dict = {}

        for idx, row in db_df.iterrows():
            if pd.notna(row['normalized_full_name']) and row['normalized_full_name'].strip():
                db_dict[row['normalized_full_name']] = idx

                # 创建语音编码
                genus_sound = jellyfish.soundex(row['normalized_genus']) if pd.notna(row['normalized_genus']) else ""
                species_sound = jellyfish.soundex(row['normalized_species']) if pd.notna(
                    row['normalized_species']) else ""
                phonetic_key = f"{genus_sound}_{species_sound}"

                if phonetic_key not in phonetic_dict:
                    phonetic_dict[phonetic_key] = []
                phonetic_dict[phonetic_key].append(idx)

        # 匹配结果存储
        matches = {
            "exact": [],
            "fuzzy": [],
            "spelling_error": [],
            "phonetic": [],
            "no_match": []
        }

        # 遍历导入数据进行匹配
        for idx, row in import_df.iterrows():
            if pd.isna(row['normalized_full_name']) or not row['normalized_full_name'].strip():
                matches["no_match"].append({
                    "import_index": idx,
                    "original_name": row.get('original_full_name', ''),
                    "reason": "invalid or null name",
                    "match_status": "no_match"
                })
                continue

            normalized_full_name = row['normalized_full_name']
            normalized_genus = row['normalized_genus']
            normalized_species = row['normalized_species']
            original_full_name = row['original_full_name']

            # 1. 完全匹配
            if normalized_full_name in db_dict:
                db_idx = db_dict[normalized_full_name]
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

            # 2. 模糊匹配（相似度 > 90%）
            best_similarity = 0
            best_match = None

            for db_name, db_idx in db_dict.items():
                # 先比较属名，如果属名相似才比较种名
                db_genus = db_name.split()[0] if ' ' in db_name else db_name
                genus_similarity = fuzz.ratio(normalized_genus, db_genus)

                if genus_similarity > 80:  # 只有属名相似才继续比较
                    similarity = fuzz.ratio(normalized_full_name, db_name)
                    if similarity > 90 and similarity > best_similarity:
                        best_similarity = similarity
                        best_match = db_idx

            if best_match is not None:
                matches["fuzzy"].append({
                    "import_index": idx,
                    "taxon_id": db_df.loc[best_match, 'TaxonID'],
                    "import_name": original_full_name,
                    "db_name": db_df.loc[best_match, 'original_full_name'],
                    "similarity": best_similarity,
                    "import_normalized": normalized_full_name,
                    "db_normalized": db_df.loc[best_match, 'normalized_full_name'],
                    "match_status": "fuzzy"
                })
                continue

            # 3. 拼写错误匹配（Levenshtein距离 <= 2对属名，<= 3对全名）
            best_distance = 999
            best_match = None

            for db_name, db_idx in db_dict.items():
                # 先比较属名
                db_genus = db_name.split()[0] if ' ' in db_name else db_name
                genus_distance = levenshtein_distance(normalized_genus, db_genus)

                if genus_distance <= 2:  # 属名编辑距离小时才继续比较
                    name_distance = levenshtein_distance(normalized_full_name, db_name)
                    if name_distance <= 3 and name_distance < best_distance:
                        best_distance = name_distance
                        best_match = db_idx

            if best_match is not None:
                matches["spelling_error"].append({
                    "import_index": idx,
                    "taxon_id": db_df.loc[best_match, 'TaxonID'],
                    "import_name": original_full_name,
                    "db_name": db_df.loc[best_match, 'original_full_name'],
                    "edit_distance": best_distance,
                    "import_normalized": normalized_full_name,
                    "db_normalized": db_df.loc[best_match, 'normalized_full_name'],
                    "match_status": "spelling_error"
                })
                continue

            # 4. 语音编码匹配
            genus_sound = jellyfish.soundex(normalized_genus) if pd.notna(normalized_genus) else ""
            species_sound = jellyfish.soundex(normalized_species) if pd.notna(normalized_species) else ""
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

            # 5. 完全不匹配
            matches["no_match"].append({
                "import_index": idx,
                "original_name": original_full_name,
                "normalized_name": normalized_full_name,
                "reason": "match not found",
                "match_status": "no_match"
            })

        # 生成匹配统计报告
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