"""
Synonym detection service for checking local taxonomic names
against the TaxonRank database.

Pipeline:
  Step 1: Spelling/phonetic correction — fuzzy match against TaxonRank names
  Step 2: Synonym resolution — check if the (corrected) name is a synonym

Issue types:
  - 'spelling'          : Name has typo, corrected name is valid (not a synonym)
  - 'synonym'           : Name is correct but is a known synonym
  - 'spelling+synonym'  : Name has typo AND corrected name is also a synonym

This module is completely separate from species_validation.py by design.
"""

import logging
import time
import uuid
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from urllib.parse import quote

from fuzzywuzzy import fuzz
import jellyfish

from app.db.database import (
    execute_query, execute_single_query, execute_mutation,
    execute_paginated_query_with_count
)
from app.db.taxon_database import execute_taxon_query, is_taxon_db_configured

logger = logging.getLogger(__name__)

# Fuzzy matching thresholds
FUZZY_THRESHOLD = 85          # minimum fuzz.ratio for fuzzy match
SPELLING_MAX_DISTANCE = 3     # max Levenshtein edit distance for spelling correction


class SynonymService:
    """Service for detecting and managing taxonomic synonyms."""

    _taxon_cache: Optional[Dict[str, Any]] = None
    _cache_timestamp: Optional[float] = None
    _cache_ttl: int = 3600  # 1 hour

    async def load_taxonrank_cache(self, force_reload: bool = False) -> Dict[str, Any]:
        """
        Load and cache the TaxonRank data with multiple lookup indices.
        """
        current_time = time.time()
        if (not force_reload
                and self._taxon_cache is not None
                and self._cache_timestamp is not None
                and current_time - self._cache_timestamp < self._cache_ttl):
            return self._taxon_cache

        if not is_taxon_db_configured():
            raise RuntimeError("TaxonRank database not configured. Set TAXON_DB_* in .env")

        query = """
            SELECT
                t.id AS taxonrank_id,
                t.scientific_name,
                t.valid_id,
                t.rank,
                t.status,
                COALESCE(v.scientific_name, t.scientific_name) AS valid_name,
                COALESCE(v.id, t.id) AS resolved_valid_id
            FROM taxa t
            LEFT JOIN taxa v ON t.valid_id = v.id
        """

        records = await execute_taxon_query(query)

        # Primary index: exact normalized name → info
        name_map = {}
        # Genus index: genus_lower → list of (full_normalized, info)
        genus_index = {}
        # Phonetic index: soundex_key → list of (full_normalized, info)
        phonetic_index = {}
        synonym_count = 0

        for record in records:
            name = record.get('scientific_name')
            if not name:
                continue
            normalized = name.strip().lower()
            parts = normalized.split()
            if not parts:
                continue

            is_synonym = (
                record['valid_id'] is not None
                and str(record['valid_id']) != str(record['taxonrank_id'])
            )

            info = {
                'scientific_name': record['scientific_name'],
                'valid_name': record['valid_name'],
                'taxonrank_id': record['taxonrank_id'],
                'valid_id': record['valid_id'],
                'resolved_valid_id': record['resolved_valid_id'],
                'rank': record.get('rank'),
                'status': record.get('status'),
                'is_synonym': is_synonym
            }

            name_map[normalized] = info

            if is_synonym:
                synonym_count += 1

            # Build genus index for efficient fuzzy search
            genus_lower = parts[0]
            if genus_lower not in genus_index:
                genus_index[genus_lower] = []
            genus_index[genus_lower].append((normalized, info))

            # Build phonetic index
            try:
                genus_soundex = jellyfish.soundex(parts[0])
                species_soundex = jellyfish.soundex(parts[1]) if len(parts) > 1 else ''
                phonetic_key = f"{genus_soundex}_{species_soundex}"
                if phonetic_key not in phonetic_index:
                    phonetic_index[phonetic_key] = []
                phonetic_index[phonetic_key].append((normalized, info))
            except Exception:
                pass

        # Group index: resolved_valid_id → all equivalent names (the accepted name + its synonyms)
        group_index = {}
        for info in name_map.values():
            gid = info.get('resolved_valid_id')
            if gid is None:
                continue
            group_index.setdefault(gid, []).append(info['scientific_name'])

        self._taxon_cache = {
            'name_map': name_map,
            'genus_index': genus_index,
            'phonetic_index': phonetic_index,
            'group_index': group_index,
            'total_count': len(records),
            'synonym_count': synonym_count
        }
        self._cache_timestamp = current_time
        logger.info(
            f"TaxonRank cache loaded: {len(records)} records, "
            f"{synonym_count} synonyms, {len(genus_index)} genera"
        )
        return self._taxon_cache

    async def resolve_group(self, name: str) -> Dict[str, Any]:
        """给一个名，返回它在 taxonomy_dev 里的等价名组（接受名 + 全部同义名）。
        用于 lots 搜索的同义词层 + 来源色标。
        返回 {found, status: 'valid'|'synonym'|None, accepted_name, names: [所有等价名]}.
        """
        if not is_taxon_db_configured():
            return {'found': False, 'status': None, 'accepted_name': None, 'names': []}
        cache = await self.load_taxonrank_cache()
        norm = (name or '').strip().lower()
        info = cache['name_map'].get(norm)
        if not info:
            return {'found': False, 'status': None, 'accepted_name': None, 'names': []}
        gid = info.get('resolved_valid_id')
        names = cache['group_index'].get(gid, [info['scientific_name']])
        return {
            'found': True,
            'status': 'synonym' if info.get('is_synonym') else (info.get('status') or 'valid'),
            'accepted_name': info.get('valid_name'),
            'names': sorted(set(names))
        }

    async def resolve_to_local_taxon(self, genus: str, species: str) -> Optional[Dict[str, Any]]:
        """Resolve a verbatim genus+species through CoF to its ACCEPTED name, then return
        the matching LOCAL TaxonomicTable taxon AT THE ACCEPTED RANK (a species accepted
        name -> the species-level row with empty Subspecies; a subspecies accepted name ->
        the row with that Subspecies). This fixes verbatim 'Anchoa mitchilli' resolving to a
        local subspecies row instead of the valid species.

        Returns None when CoF doesn't know the name (caller should fall back to local match).
        Otherwise {accepted_name, status, taxon_id, family_name, local_found}; taxon_id/
        local_found are None/False when CoF has an accepted name the local table lacks.
        """
        name = " ".join(x for x in [(genus or "").strip(), (species or "").strip()] if x).strip()
        if not name:
            return None
        try:
            r = await self.resolve_group(name)
        except Exception as e:  # noqa: BLE001
            logger.warning("resolve_to_local_taxon: resolve_group failed for %r: %s", name, e)
            return None
        if not r.get("found") or not r.get("accepted_name"):
            return None  # not in CoF -> caller falls back to existing local matching

        accepted = r["accepted_name"].strip()
        parts = accepted.split()
        g = parts[0] if parts else ""
        s = parts[1] if len(parts) > 1 else ""
        sub = " ".join(parts[2:]) if len(parts) > 2 else ""

        if sub:
            rows = await execute_query(
                'SELECT tt."TaxonID", f."FamilyName" FROM "TaxonomicTable" tt '
                'LEFT JOIN "Family" f ON tt."FamilyID" = f."FamilyID" '
                'WHERE lower(tt."Genus") = lower($1) AND lower(tt."Species") = lower($2) '
                'AND lower(COALESCE(tt."Subspecies", \'\')) = lower($3) '
                'ORDER BY tt."TaxonID" LIMIT 1', g, s, sub)
        else:
            rows = await execute_query(
                'SELECT tt."TaxonID", f."FamilyName" FROM "TaxonomicTable" tt '
                'LEFT JOIN "Family" f ON tt."FamilyID" = f."FamilyID" '
                'WHERE lower(tt."Genus") = lower($1) AND lower(tt."Species") = lower($2) '
                'AND (tt."Subspecies" IS NULL OR TRIM(tt."Subspecies") = \'\') '
                'ORDER BY tt."TaxonID" LIMIT 1', g, s)

        out = {
            "accepted_name": accepted,
            "status": r.get("status"),
            "taxon_id": rows[0]["TaxonID"] if rows else None,
            "family_name": rows[0]["FamilyName"] if rows else None,
            "local_found": bool(rows),
            "cof_genus": g,
            "cof_species": s,
            "cof_subspecies": sub or None,
            "cof_family": None,
        }
        # 本地没有该 accepted taxon -> 附上 CoF 的科名（取 accepted genus 在 CoF 的 FAMILY），
        # 供 review 端"建议创建 CoF 名"。
        if not rows and g:
            try:
                fam = await execute_taxon_query(
                    "SELECT fam.scientific_name AS family "
                    "FROM taxa gg JOIN taxa fam ON gg.parent_id = fam.id AND fam.rank = 'FAMILY' "
                    "WHERE gg.rank = 'GENUS' AND lower(gg.scientific_name) = lower($1) LIMIT 1", g)
                out["cof_family"] = fam[0]["family"] if fam else None
            except Exception:
                out["cof_family"] = None
        return out

    async def tag_status(self, names: List[str]) -> Dict[str, Any]:
        """批量给一组名打 taxonomy_dev 状态标：返回 {normalized_name: 'valid'|'synonym'}。
        用于 lots 搜索结果的来源色标。taxonomy_dev 未配置/连不上则返回空（无标签）。
        """
        if not is_taxon_db_configured():
            return {}
        try:
            cache = await self.load_taxonrank_cache()
        except Exception:
            return {}
        nm = cache['name_map']
        out: Dict[str, Any] = {}
        for n in names:
            if not n:
                continue
            norm = str(n).strip().lower()
            if norm in out:
                continue
            info = nm.get(norm)
            if info:
                out[norm] = 'synonym' if info.get('is_synonym') else 'valid'
        return out

    def _find_best_fuzzy_match(
        self, search_name: str, genus_index: Dict, phonetic_index: Dict
    ) -> Optional[Tuple[str, Dict, str, float]]:
        """
        Find best fuzzy/phonetic match for a name that wasn't found exactly.

        Returns: (matched_name, info, correction_type, confidence) or None
        """
        parts = search_name.lower().split()
        if not parts:
            return None

        search_genus = parts[0]
        search_normalized = search_name.lower()

        best_match = None
        best_score = 0

        # Strategy 1: Fuzzy match within same genus (handles species-level typos)
        if search_genus in genus_index:
            for db_name, info in genus_index[search_genus]:
                score = fuzz.ratio(search_normalized, db_name)
                if score >= FUZZY_THRESHOLD and score > best_score:
                    best_score = score
                    best_match = (info['scientific_name'], info, 'fuzzy', score / 100.0)

        # Strategy 2: Fuzzy match with similar genera (handles genus-level typos)
        if best_score < FUZZY_THRESHOLD:
            for genus_key, entries in genus_index.items():
                # Only check genera with small edit distance
                if jellyfish.levenshtein_distance(search_genus, genus_key) <= 2:
                    for db_name, info in entries:
                        score = fuzz.ratio(search_normalized, db_name)
                        if score >= FUZZY_THRESHOLD and score > best_score:
                            best_score = score
                            best_match = (info['scientific_name'], info, 'fuzzy', score / 100.0)

        # Strategy 3: Phonetic match (handles pronunciation-based errors)
        if best_score < FUZZY_THRESHOLD:
            try:
                genus_soundex = jellyfish.soundex(parts[0])
                species_soundex = jellyfish.soundex(parts[1]) if len(parts) > 1 else ''
                phonetic_key = f"{genus_soundex}_{species_soundex}"

                if phonetic_key in phonetic_index:
                    for db_name, info in phonetic_index[phonetic_key]:
                        score = fuzz.ratio(search_normalized, db_name)
                        if score > best_score:
                            best_score = score
                            best_match = (info['scientific_name'], info, 'phonetic', score / 100.0)
            except Exception:
                pass

        # Only return if we found something reasonable
        if best_match and best_score >= 70:
            return best_match
        return None

    async def scan_all_taxon_names(self) -> Dict[str, Any]:
        """
        Scan ALL names in TaxonomicTable against TaxonRank DB.

        Pipeline per name:
          1. Exact match → check synonym
          2. If no exact match → fuzzy/phonetic correction → check synonym on corrected name
        """
        scan_batch_id = datetime.now().strftime("%Y%m%d") + "-" + str(uuid.uuid4())[:6]

        # 1. Load TaxonRank cache
        cache = await self.load_taxonrank_cache(force_reload=True)
        name_map = cache['name_map']
        genus_index = cache['genus_index']
        phonetic_index = cache['phonetic_index']

        # 2. Load all local taxon names
        local_query = """
            SELECT tt."TaxonID", tt."Genus", tt."Species", tt."Subspecies",
                   tt."FullScientificName", tt."FamilyID", tt."Remarks",
                   f."FamilyName"
            FROM "TaxonomicTable" tt
            LEFT JOIN "Family" f ON tt."FamilyID" = f."FamilyID"
            ORDER BY tt."TaxonID"
        """
        local_records = await execute_query(local_query)

        # 3. Clear existing pending reviews
        await execute_mutation(
            "DELETE FROM taxon_synonym_review WHERE review_status = 'pending'"
        )

        # 4. Process each name
        detected = []
        stats = {
            'total_checked': 0,
            'synonyms_found': 0,
            'spelling_corrections': 0,
            'spelling_and_synonym': 0,
            'already_valid': 0,
            'not_found_in_taxonrank': 0,
            'skipped_empty': 0
        }

        for record in local_records:
            stats['total_checked'] += 1

            genus = (record.get('Genus') or '').strip()
            species = (record.get('Species') or '').strip()
            full_name = (record.get('FullScientificName') or '').strip()

            search_name = f"{genus} {species}".strip()
            if not search_name or not genus:
                stats['skipped_empty'] += 1
                continue

            normalized = search_name.lower()

            # --- Step 1: Try exact match ---
            taxon_info = name_map.get(normalized)

            if taxon_info is not None:
                # Exact match found
                if not taxon_info['is_synonym']:
                    stats['already_valid'] += 1
                    continue

                # It's a synonym
                stats['synonyms_found'] += 1
                valid_name = taxon_info['valid_name'] or ''
                valid_parts = valid_name.split(None, 1)

                detected.append(self._build_review_item(
                    record=record, full_name=full_name, genus=genus, species=species,
                    search_name=search_name,
                    corrected_name=None, corrected_genus=None, corrected_species=None,
                    correction_type=None, correction_confidence=None,
                    valid_name=valid_name,
                    suggested_genus=valid_parts[0] if valid_parts else None,
                    suggested_species=valid_parts[1] if len(valid_parts) > 1 else None,
                    taxon_info=taxon_info,
                    issue_type='synonym',
                    detection_method='exact',
                    match_confidence=1.0,
                    scan_batch_id=scan_batch_id
                ))
                continue

            # --- Step 2: Fuzzy/phonetic match (spelling correction) ---
            fuzzy_result = self._find_best_fuzzy_match(
                search_name, genus_index, phonetic_index
            )

            if fuzzy_result is None:
                stats['not_found_in_taxonrank'] += 1
                continue

            matched_name, matched_info, correction_type, correction_conf = fuzzy_result
            matched_parts = matched_name.split(None, 1)
            corrected_genus = matched_parts[0] if matched_parts else None
            corrected_species = matched_parts[1] if len(matched_parts) > 1 else None

            if matched_info['is_synonym']:
                # Spelling error + synonym
                stats['spelling_and_synonym'] += 1
                valid_name = matched_info['valid_name'] or ''
                valid_parts = valid_name.split(None, 1)

                detected.append(self._build_review_item(
                    record=record, full_name=full_name, genus=genus, species=species,
                    search_name=search_name,
                    corrected_name=matched_name,
                    corrected_genus=corrected_genus,
                    corrected_species=corrected_species,
                    correction_type=correction_type,
                    correction_confidence=correction_conf,
                    valid_name=valid_name,
                    suggested_genus=valid_parts[0] if valid_parts else None,
                    suggested_species=valid_parts[1] if len(valid_parts) > 1 else None,
                    taxon_info=matched_info,
                    issue_type='spelling+synonym',
                    detection_method=correction_type,
                    match_confidence=correction_conf,
                    scan_batch_id=scan_batch_id
                ))
            else:
                # Just a spelling error, corrected name is valid
                stats['spelling_corrections'] += 1

                detected.append(self._build_review_item(
                    record=record, full_name=full_name, genus=genus, species=species,
                    search_name=search_name,
                    corrected_name=matched_name,
                    corrected_genus=corrected_genus,
                    corrected_species=corrected_species,
                    correction_type=correction_type,
                    correction_confidence=correction_conf,
                    valid_name=matched_name,  # corrected name IS the valid name
                    suggested_genus=corrected_genus,
                    suggested_species=corrected_species,
                    taxon_info=matched_info,
                    issue_type='spelling',
                    detection_method=correction_type,
                    match_confidence=correction_conf,
                    scan_batch_id=scan_batch_id
                ))

        # 5. Insert detected issues
        for item in detected:
            await execute_mutation(
                """
                INSERT INTO taxon_synonym_review (
                    taxon_id, current_full_name, current_genus, current_species,
                    current_family, current_author,
                    corrected_full_name, corrected_genus, corrected_species,
                    correction_type, correction_confidence,
                    suggested_full_name, suggested_genus, suggested_species, suggested_author,
                    taxonrank_id, taxonrank_valid_id,
                    issue_type, detection_method, match_confidence, synonym_type,
                    worms_url, scan_batch_id, review_status
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                    $21, $22, $23, 'pending'
                )
                ON CONFLICT (taxon_id) WHERE review_status = 'pending'
                DO UPDATE SET
                    corrected_full_name = EXCLUDED.corrected_full_name,
                    corrected_genus = EXCLUDED.corrected_genus,
                    corrected_species = EXCLUDED.corrected_species,
                    correction_type = EXCLUDED.correction_type,
                    correction_confidence = EXCLUDED.correction_confidence,
                    suggested_full_name = EXCLUDED.suggested_full_name,
                    suggested_genus = EXCLUDED.suggested_genus,
                    suggested_species = EXCLUDED.suggested_species,
                    taxonrank_id = EXCLUDED.taxonrank_id,
                    taxonrank_valid_id = EXCLUDED.taxonrank_valid_id,
                    issue_type = EXCLUDED.issue_type,
                    detection_method = EXCLUDED.detection_method,
                    match_confidence = EXCLUDED.match_confidence,
                    synonym_type = EXCLUDED.synonym_type,
                    worms_url = EXCLUDED.worms_url,
                    scan_batch_id = EXCLUDED.scan_batch_id,
                    updated_at = CURRENT_TIMESTAMP
                """,
                item['taxon_id'], item['current_full_name'],
                item['current_genus'], item['current_species'],
                item['current_family'], item['current_author'],
                item['corrected_full_name'], item['corrected_genus'],
                item['corrected_species'], item['correction_type'],
                item['correction_confidence'],
                item['suggested_full_name'], item['suggested_genus'],
                item['suggested_species'], item['suggested_author'],
                item['taxonrank_id'], item['taxonrank_valid_id'],
                item['issue_type'], item['detection_method'],
                item['match_confidence'], item['synonym_type'],
                item['worms_url'], item['scan_batch_id']
            )

        return {
            'scan_batch_id': scan_batch_id,
            'stats': stats,
            'total_detected': len(detected),
            'taxonrank_total': cache['total_count'],
            'taxonrank_synonyms': cache['synonym_count']
        }

    @staticmethod
    def _build_review_item(
        record, full_name, genus, species, search_name,
        corrected_name, corrected_genus, corrected_species,
        correction_type, correction_confidence,
        valid_name, suggested_genus, suggested_species,
        taxon_info, issue_type, detection_method, match_confidence,
        scan_batch_id
    ) -> Dict[str, Any]:
        return {
            'taxon_id': record['TaxonID'],
            'current_full_name': full_name or search_name,
            'current_genus': genus,
            'current_species': species,
            'current_family': record.get('FamilyName'),
            'current_author': None,
            'corrected_full_name': corrected_name,
            'corrected_genus': corrected_genus,
            'corrected_species': corrected_species,
            'correction_type': correction_type,
            'correction_confidence': correction_confidence,
            'suggested_full_name': valid_name,
            'suggested_genus': suggested_genus,
            'suggested_species': suggested_species,
            'suggested_author': None,
            'taxonrank_id': taxon_info['taxonrank_id'],
            'taxonrank_valid_id': taxon_info['resolved_valid_id'],
            'issue_type': issue_type,
            'detection_method': detection_method,
            'match_confidence': match_confidence,
            'synonym_type': taxon_info.get('status', 'synonym'),
            'worms_url': SynonymService.generate_worms_url(search_name),
            'scan_batch_id': scan_batch_id
        }

    # ==================== Review CRUD ====================

    async def get_review_items(
        self,
        page: int = 1,
        page_size: int = 20,
        status_filter: Optional[str] = None,
        issue_filter: Optional[str] = None,
        search: Optional[str] = None,
        sort_by: str = "created_at",
        sort_order: str = "desc"
    ) -> Dict[str, Any]:
        """Get paginated list of review items."""
        where_clauses = []
        params = []
        param_idx = 1

        if status_filter:
            where_clauses.append(f"review_status = ${param_idx}")
            params.append(status_filter)
            param_idx += 1

        if issue_filter:
            where_clauses.append(f"issue_type = ${param_idx}")
            params.append(issue_filter)
            param_idx += 1

        if search:
            where_clauses.append(
                f"(current_full_name ILIKE ${param_idx} "
                f"OR corrected_full_name ILIKE ${param_idx} "
                f"OR suggested_full_name ILIKE ${param_idx})"
            )
            params.append(f"%{search}%")
            param_idx += 1

        where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        allowed_sort = {
            "created_at", "current_full_name", "suggested_full_name",
            "review_status", "issue_type", "match_confidence", "taxon_id"
        }
        if sort_by not in allowed_sort:
            sort_by = "created_at"
        sort_dir = "ASC" if sort_order.lower() == "asc" else "DESC"

        main_query = f"""
            SELECT id, taxon_id, current_full_name, current_genus, current_species,
                   current_family,
                   corrected_full_name, corrected_genus, corrected_species,
                   correction_type, correction_confidence,
                   suggested_full_name, suggested_genus, suggested_species,
                   issue_type, detection_method, match_confidence, synonym_type,
                   worms_url, review_status, reviewed_by, reviewed_at, review_notes,
                   final_valid_name, correction_source,
                   scan_batch_id, created_at, updated_at
            FROM taxon_synonym_review
            {where_sql}
            ORDER BY {sort_by} {sort_dir}
        """

        count_query = f"SELECT COUNT(*) FROM taxon_synonym_review {where_sql}"

        return await execute_paginated_query_with_count(
            main_query, count_query, params, page, page_size
        )

    async def get_review_detail(self, review_id: int) -> Optional[Dict[str, Any]]:
        """Get detailed information about a single review item."""
        review = await execute_single_query(
            "SELECT * FROM taxon_synonym_review WHERE id = $1",
            review_id
        )
        if not review:
            return None

        # Get usage count
        usage = await execute_single_query(
            'SELECT COUNT(*) AS usage_count FROM "Primary" WHERE "TaxonID" = $1',
            review['taxon_id']
        )

        result = {}
        for key, value in review.items():
            if isinstance(value, datetime):
                result[key] = value.isoformat()
            else:
                result[key] = value

        result['usage_count'] = usage['usage_count'] if usage else 0
        result['worms_current_url'] = self.generate_worms_url(review['current_full_name'])
        if review.get('corrected_full_name'):
            result['worms_corrected_url'] = self.generate_worms_url(review['corrected_full_name'])
        if review.get('suggested_full_name'):
            result['worms_suggested_url'] = self.generate_worms_url(review['suggested_full_name'])

        return result

    async def accept_synonym(
        self, review_id: int, reviewed_by: str,
        notes: Optional[str] = None, final_valid_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Accept a suggestion (record only, does not modify TaxonomicTable).
        If final_valid_name is provided, it overrides the auto-suggested name."""
        review = await execute_single_query(
            "SELECT id, review_status, suggested_full_name FROM taxon_synonym_review WHERE id = $1",
            review_id
        )
        if not review:
            return {'error': 'Review not found'}

        # Use provided name or fall back to suggested
        resolved_name = final_valid_name.strip() if final_valid_name else review['suggested_full_name']

        now = datetime.now()
        await execute_mutation(
            """
            UPDATE taxon_synonym_review
            SET review_status = 'accepted', reviewed_by = $1, reviewed_at = $2,
                review_notes = $3, final_valid_name = $4, updated_at = $2
            WHERE id = $5
            """,
            reviewed_by, now, notes, resolved_name, review_id
        )
        return {'review_id': review_id, 'status': 'accepted', 'final_valid_name': resolved_name}

    async def reject_synonym(
        self, review_id: int, reviewed_by: str,
        notes: Optional[str] = None, final_valid_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Reject a suggestion.
        If final_valid_name is provided, it means the reviewer wants to use a different name.
        If final_valid_name is None/empty, it means keeping the original name (suggestion was wrong)."""
        review = await execute_single_query(
            "SELECT id, review_status FROM taxon_synonym_review WHERE id = $1",
            review_id
        )
        if not review:
            return {'error': 'Review not found'}

        resolved_name = final_valid_name.strip() if final_valid_name else None
        now = datetime.now()
        await execute_mutation(
            """
            UPDATE taxon_synonym_review
            SET review_status = 'rejected', reviewed_by = $1, reviewed_at = $2,
                review_notes = $3, final_valid_name = $4, updated_at = $2
            WHERE id = $5
            """,
            reviewed_by, now, notes, resolved_name, review_id
        )
        return {'review_id': review_id, 'status': 'rejected', 'final_valid_name': resolved_name}

    async def skip_synonym(
        self, review_id: int, reviewed_by: str, notes: Optional[str] = None
    ) -> Dict[str, Any]:
        """Skip (defer) a review."""
        review = await execute_single_query(
            "SELECT id, review_status FROM taxon_synonym_review WHERE id = $1",
            review_id
        )
        if not review:
            return {'error': 'Review not found'}

        now = datetime.now()
        await execute_mutation(
            """
            UPDATE taxon_synonym_review
            SET review_status = 'skipped', reviewed_by = $1, reviewed_at = $2,
                review_notes = $3, updated_at = $2
            WHERE id = $4
            """,
            reviewed_by, now, notes, review_id
        )
        return {'review_id': review_id, 'status': 'skipped'}

    async def batch_accept(
        self, review_ids: List[int], reviewed_by: str, notes: Optional[str] = None
    ) -> Dict[str, Any]:
        """Batch accept multiple suggestions."""
        now = datetime.now()
        success_count = 0
        skipped = []

        for rid in review_ids:
            review = await execute_single_query(
                "SELECT id FROM taxon_synonym_review WHERE id = $1", rid
            )
            if not review:
                skipped.append(rid)
                continue
            await execute_mutation(
                """
                UPDATE taxon_synonym_review
                SET review_status = 'accepted', reviewed_by = $1, reviewed_at = $2,
                    review_notes = $3, updated_at = $2
                WHERE id = $4
                """,
                reviewed_by, now, notes, rid
            )
            success_count += 1

        return {
            'accepted_count': success_count,
            'skipped_ids': skipped,
            'total_requested': len(review_ids)
        }

    async def batch_reject(
        self, review_ids: List[int], reviewed_by: str, notes: Optional[str] = None
    ) -> Dict[str, Any]:
        """Batch reject multiple suggestions."""
        now = datetime.now()
        success_count = 0
        skipped = []

        for rid in review_ids:
            review = await execute_single_query(
                "SELECT id FROM taxon_synonym_review WHERE id = $1", rid
            )
            if not review:
                skipped.append(rid)
                continue
            await execute_mutation(
                """
                UPDATE taxon_synonym_review
                SET review_status = 'rejected', reviewed_by = $1, reviewed_at = $2,
                    review_notes = $3, updated_at = $2
                WHERE id = $4
                """,
                reviewed_by, now, notes, rid
            )
            success_count += 1

        return {
            'rejected_count': success_count,
            'skipped_ids': skipped,
            'total_requested': len(review_ids)
        }

    async def correct_synonym(
        self, review_id: int, reviewed_by: str,
        final_valid_name: str,
        notes: Optional[str] = None,
        final_genus: Optional[str] = None,
        final_species: Optional[str] = None,
        correction_source: Optional[str] = None,
        taxonrank_ref_id: Optional[int] = None,
        create_in_local: bool = False
    ) -> Dict[str, Any]:
        """Correct a review with a reviewer-provided valid name."""
        review = await execute_single_query(
            "SELECT id, taxon_id FROM taxon_synonym_review WHERE id = $1",
            review_id
        )
        if not review:
            return {'error': 'Review not found'}

        now = datetime.now()
        await execute_mutation(
            """
            UPDATE taxon_synonym_review
            SET review_status = 'corrected', reviewed_by = $1, reviewed_at = $2,
                review_notes = $3, final_valid_name = $4,
                correction_source = $5, taxonrank_ref_id = $6, updated_at = $2
            WHERE id = $7
            """,
            reviewed_by, now, notes, final_valid_name.strip(),
            correction_source, taxonrank_ref_id, review_id
        )

        result = {
            'review_id': review_id,
            'status': 'corrected',
            'final_valid_name': final_valid_name.strip()
        }

        # Optionally create the taxon in the local TaxonomicTable
        if create_in_local and final_genus:
            try:
                insert_result = await execute_query(
                    """
                    INSERT INTO "TaxonomicTable" (
                        "Genus", "Species", "FullScientificName",
                        created_at, created_via
                    )
                    VALUES ($1, $2, $3, NOW(), 'synonym_correction')
                    RETURNING "TaxonID"
                    """,
                    final_genus, final_species or '', final_valid_name.strip()
                )
                if insert_result:
                    result['created_taxon_id'] = insert_result[0]['TaxonID']
            except Exception as e:
                result['create_warning'] = f"Taxon creation failed: {str(e)}"

        return result

    async def reset_synonym(
        self, review_id: int, reviewed_by: str
    ) -> Dict[str, Any]:
        """Reset a processed review back to pending status."""
        review = await execute_single_query(
            "SELECT id, review_status FROM taxon_synonym_review WHERE id = $1",
            review_id
        )
        if not review:
            return {'error': 'Review not found'}
        if review['review_status'] == 'pending':
            return {'error': 'Review is already pending'}

        now = datetime.now()
        await execute_mutation(
            """
            UPDATE taxon_synonym_review
            SET review_status = 'pending', reviewed_by = NULL, reviewed_at = NULL,
                review_notes = NULL, final_valid_name = NULL,
                correction_source = NULL, taxonrank_ref_id = NULL, updated_at = $1
            WHERE id = $2
            """,
            now, review_id
        )
        return {'review_id': review_id, 'status': 'pending'}

    # ==================== Stats ====================

    async def get_stats(self) -> Dict[str, Any]:
        """Get review statistics."""
        stats = await execute_single_query("""
            SELECT
                COUNT(*) AS total_reviews,
                SUM(CASE WHEN review_status = 'pending' THEN 1 ELSE 0 END) AS pending,
                SUM(CASE WHEN review_status = 'accepted' THEN 1 ELSE 0 END) AS accepted,
                SUM(CASE WHEN review_status = 'rejected' THEN 1 ELSE 0 END) AS rejected,
                SUM(CASE WHEN review_status = 'skipped' THEN 1 ELSE 0 END) AS skipped,
                SUM(CASE WHEN review_status = 'corrected' THEN 1 ELSE 0 END) AS corrected,
                SUM(CASE WHEN issue_type = 'spelling' THEN 1 ELSE 0 END) AS spelling_issues,
                SUM(CASE WHEN issue_type = 'synonym' THEN 1 ELSE 0 END) AS synonym_issues,
                SUM(CASE WHEN issue_type = 'spelling+synonym' THEN 1 ELSE 0 END) AS spelling_synonym_issues,
                MAX(created_at) AS last_scan_date
            FROM taxon_synonym_review
        """)

        taxon_count = await execute_single_query(
            'SELECT COUNT(*) AS count FROM "TaxonomicTable"'
        )

        result = {}
        if stats:
            for key, value in stats.items():
                if isinstance(value, datetime):
                    result[key] = value.isoformat()
                else:
                    result[key] = value
        else:
            result = {
                'total_reviews': 0, 'pending': 0, 'accepted': 0,
                'rejected': 0, 'skipped': 0, 'corrected': 0,
                'spelling_issues': 0, 'synonym_issues': 0,
                'spelling_synonym_issues': 0, 'last_scan_date': None
            }

        result['total_taxon_count'] = taxon_count['count'] if taxon_count else 0
        result['taxon_db_configured'] = is_taxon_db_configured()

        return result

    @staticmethod
    def generate_worms_url(scientific_name: str) -> str:
        """Generate WoRMS search URL for a scientific name."""
        if not scientific_name:
            return ""
        return f"https://www.marinespecies.org/aphia.php?p=taxlist&tName={quote(scientific_name)}"