import os
import re
from collections import defaultdict
from flask import Flask, jsonify, send_from_directory
from azure.data.tables import TableServiceClient
from dotenv import load_dotenv

load_dotenv()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# configuration is read lazily so the module can be imported without
# immediately blowing up when the environment is not yet wired up.  this
# makes unit testing and tooling easier.  the helper functions b
# elow will
# initialize the client on first use and will raise the same errors that
# previously occurred at import time.

TABLE_NAME = os.getenv('TABLE_NAME', 'NEATevalautions')  # kept the original

# placeholder for the table client, created on demand
client = None


def _init_client():
    """Create and return a TableClient using environment variables.

    Raises :class:`RuntimeError` if the required credentials are missing.
    """
    account = (os.getenv('AZURE_STORAGE_ACCOUNT') or '').strip()
    key = (os.getenv('AZURE_STORAGE_KEY') or '').strip()
    endpoint = (os.getenv('AZURE_TABLES_ENDPOINT') or '').strip()

    if not key:
        raise RuntimeError('Missing AZURE_STORAGE_KEY')

    if endpoint:
        svc = TableServiceClient(endpoint=endpoint, credential=key)
        return svc.get_table_client(TABLE_NAME)

    if not account:
        raise RuntimeError('Missing AZURE_STORAGE_ACCOUNT (or set AZURE_TABLES_ENDPOINT)')

    account = account.replace('https://', '').split('.')[0]

    conn_str = (
        f'DefaultEndpointsProtocol=https;AccountName={account};'
        f'AccountKey={key};EndpointSuffix=core.windows.net'
    )
    svc = TableServiceClient.from_connection_string(conn_str)
    return svc.get_table_client(TABLE_NAME)


def _ensure_client():
    """Guarantee that ``client`` is ready for use and return it.

    This is idempotent and safe to call from request handlers.
    """
    global client
    if client is None:
        client = _init_client()
    return client

app = Flask(__name__, static_folder=BASE_DIR, static_url_path='')

# ── Column classification patterns ────────────────────────────────────────────
# Actual column names from Microsoft Forms have no spaces / special chars, e.g.:
#   HPIEvaluationQuestionsThoroughnoomissions
#   SOCIALHISTORYCorrectcaptureofspecialtycontentrequirements
#   Version1HPIEvaluationQuestionsAccurateincludingnohallucinations
#
# Order matters: longer prefixes first so "SOCIALHISTORY" matches before a
# hypothetical "SOCIAL" catch-all, and "REVIEWOFSYSTEMS" before "RESULTS".

SECTION_PATTERNS = [
    ('social_history', re.compile(r'SOCIALHISTORY',                 re.I)),
    ('family_history', re.compile(r'FAMILYHISTORY',                 re.I)),
    ('physical_exam',  re.compile(r'PHYSICALEXAM',                  re.I)),
    ('ap',             re.compile(r'ASSESSMENTANDPLAN|ASSEEEMENTANDPLAN', re.I)),
    ('ros',            re.compile(r'REVIEWOFSYSTEMS',               re.I)),
    ('immunizations',  re.compile(r'IMMUNIZATIONS',                 re.I)),
    ('medications',    re.compile(r'MEDICATIONS',                   re.I)),
    ('allergies',      re.compile(r'ALLERGIES',                     re.I)),
    ('results',        re.compile(r'RESULTS',                       re.I)),
    ('hpi',            re.compile(r'HPI',                           re.I)),
]

ATTR_PATTERNS = {
    'Organized':     re.compile(r'Organized',    re.I),
    'Thorough':      re.compile(r'Thorough',     re.I),
    'Accurate':      re.compile(r'Accurate',     re.I),
    'Comprehensive': re.compile(r'Comprehensive', re.I),
    'Synthesized':   re.compile(r'Synthesized',  re.I),
    'Citation':      re.compile(r'Citation',     re.I),
}

# Columns that are NOT attribute scores — skip them during section classification
SKIP_COL_PATTERNS = re.compile(
    r'Iftie|Whatversion|Correctcapture|AdditionalComments'
    r'|^Id$|^Starttime$|^Completiontime$|^Email$|^Name$'
    r'|^SelectRegion$|^EnterBenchnarkName$|^CIUserName$'
    r'|^NEATEncounterID$|^NEATSpecialty$|^Howmanyversions',
    re.I
)

# Azure system columns to skip
SYSTEM_COLS = {'PartitionKey', 'RowKey', 'Timestamp', 'etag', 'odata.etag'}


def coerce_float(val):
    """Convert val to a float on [0, 100], or return None."""
    if isinstance(val, bool):
        return 100.0 if val else 0.0
    if isinstance(val, (int, float)):
        v = float(val)
        # If on a 0-1 scale, promote to 0-100
        return v * 100 if 0 < v <= 1 else v
    if isinstance(val, str):
        s = val.strip().rstrip('%')
        try:
            v = float(s)
            return v * 100 if 0 < v <= 1 else v
        except ValueError:
            pass
        lw = s.lower()
        if lw in {'yes', 'pass', 'true', 'correct', '1'}:
            return 100.0
        if lw in {'no', 'fail', 'false', 'incorrect', '0'}:
            return 0.0
    return None


def classify_columns(columns):
    """
    Return a dict: col_name -> {'section': sec_key, 'attr': attr_key | None,
                                 'version': 1|2|None}
    for every column that matches a known section + attribute pattern.
    Columns matching SKIP_COL_PATTERNS (tie questions, correct-capture, metadata)
    are excluded so only actual attribute evaluation columns are mapped.
    """
    mapping = {}
    for col in columns:
        if col in SYSTEM_COLS:
            continue
        if SKIP_COL_PATTERNS.search(col):
            continue

        # Detect version prefix
        version = None
        col_rest = col
        m = re.match(r'^Version(\d)', col, re.I)
        if m:
            version = int(m.group(1))
            col_rest = col[len(m.group(0)):]  # strip "Version1" / "Version2"

        # Match section (order matters — longest prefixes first)
        sec = None
        for sec_key, pat in SECTION_PATTERNS:
            if pat.search(col_rest):
                sec = sec_key
                break
        if sec is None:
            continue

        # Match attribute
        attr = next((k for k, p in ATTR_PATTERNS.items() if p.search(col)), None)
        if attr is None:
            continue  # no recognized attribute — skip

        mapping[col] = {'section': sec, 'attr': attr, 'version': version}
    return mapping


def find_col(cols, pattern):
    return next((c for c in cols if pattern.search(c)), None)


def _normalize_col_name(name):
    return re.sub(r'[^a-z0-9]', '', str(name).lower())


def find_version_column(columns):
    """Find the versions-selection column deterministically.

    Prioritizes the explicit column shared by the user screenshot:
    "Howmanyversionsareyouevaluating".
    """
    preferred = 'howmanyversionsareyouevaluating'
    for col in sorted(columns):
        if _normalize_col_name(col) == preferred:
            return col

    patterns = [
        re.compile(r'howmanyversionsareyouevaluating', re.I),
        re.compile(r'howmany.?versions', re.I),
        re.compile(r'version.?count', re.I),
    ]
    for pat in patterns:
        match = find_col(sorted(columns), pat)
        if match:
            return match
    return None


def parse_version_choice(val):
    """Parse pathway selector value as discrete choice 1 or 2."""
    if val is None:
        return None
    if isinstance(val, bool):
        return None
    if isinstance(val, (int, float)):
        as_int = int(round(float(val)))
        return as_int if as_int in (1, 2) else None

    txt = str(val).strip().lower()
    if txt in {'1', '1.0', 'one', 'single'}:
        return 1
    if txt in {'2', '2.0', 'two', 'ab', 'a/b'}:
        return 2

    m = re.search(r'\b([12])\b', txt)
    if m:
        return int(m.group(1))
    return None


def safe_avg(lst):
    return round(sum(lst) / len(lst)) if lst else None


def compute_section_data(rows, col_map):
    """
    Aggregate section scores and attribute scores across all rows.
    Only uses single-version columns (version=None) — the "base" evaluation
    columns without a Version1/Version2 prefix.
    """
    # sec_vals[sec_key][attr_key] -> [float values]
    sec_vals = defaultdict(lambda: defaultdict(list))

    for row in rows:
        for col, meta in col_map.items():
            if meta['version'] is not None:
                continue  # skip version-specific columns
            v = coerce_float(row.get(col))
            if v is None:
                continue
            sec_vals[meta['section']][meta['attr']].append(v)

    section_data = {}
    for sec_key, attr_map in sec_vals.items():
        attr_avgs = {
            a: round(safe_avg(v))
            for a, v in attr_map.items()
            if v
        }
        if not attr_avgs:
            continue

        score    = safe_avg(list(attr_avgs.values()))
        accuracy = attr_avgs.get('Accurate', min(100, round(score * 1.02)))
        thorough = attr_avgs.get('Thorough',  min(100, round(score * 0.97)))

        section_data[sec_key] = {
            'score':    min(100, round(score)),
            'accuracy': min(100, accuracy),
            'thorough': min(100, thorough),
            'attrs':    attr_avgs,
        }
    return section_data


def compute_specialty_heatmap(rows, col_map, specialty_col):
    """
    Build per-specialty, per-section score matrix for the heatmap.
    Returns {'specialties': [...], 'score': [[...]], 'accuracy': [[...]], 'thorough': [[...]]}
    """
    if not specialty_col:
        return {}

    # sp -> sec_key -> attr -> [values]
    sp_sec = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for row in rows:
        sp = str(row.get(specialty_col, 'Unknown')).strip() or 'Unknown'
        for col, meta in col_map.items():
            if meta['version'] is not None:
                continue  # skip version-specific columns
            v = coerce_float(row.get(col))
            if v is None:
                continue
            sp_sec[sp][meta['section']][meta['attr']].append(v)

    specialties = sorted(sp_sec.keys())
    SECTION_ORDER = [
        'hpi', 'medications', 'allergies', 'immunizations', 'social_history',
        'family_history', 'ros', 'physical_exam', 'results', 'ap'
    ]

    def row_for_metric(sp, metric):
        out = []
        for sec in SECTION_ORDER:
            attr_map = sp_sec[sp].get(sec, {})
            if metric == 'score':
                all_vals = [v for vals in attr_map.values() for v in vals]
                v = safe_avg(all_vals)
            elif metric == 'accuracy':
                v = safe_avg(attr_map.get('Accurate', []))
            elif metric == 'thorough':
                v = safe_avg(attr_map.get('Thorough', []))
            out.append(v if v is not None else None)
        return out

    return {
        'specialties': specialties,
        'score':    [row_for_metric(sp, 'score')    for sp in specialties],
        'accuracy': [row_for_metric(sp, 'accuracy') for sp in specialties],
        'thorough': [row_for_metric(sp, 'thorough') for sp in specialties],
    }


def compute_version_data(rows, col_map, version_col):
    """
    Build per-section A/B comparison stats using Version1/Version2 columns.
    Also uses the per-section "If tie" and "What version" columns.
    """
    if not version_col:
        return []

    ab_rows = [r for r in rows if parse_version_choice(r.get(version_col)) == 2]
    if not ab_rows:
        return []

    SECTION_LABELS = {
        'hpi': 'HPI', 'medications': 'Medications', 'allergies': 'Allergies',
        'immunizations': 'Immunizations', 'social_history': 'Social History',
        'family_history': 'Family History', 'ros': 'ROS',
        'physical_exam': 'Physical Exam', 'results': 'Results', 'ap': 'A&P',
    }

    # Group version-specific columns by section and version
    v_cols = defaultdict(lambda: {1: [], 2: []})  # sec_key -> {1: [cols], 2: [cols]}
    for col, meta in col_map.items():
        if meta['version'] in (1, 2):
            v_cols[meta['section']][meta['version']].append(col)

    # Find tie-type and winner columns per section
    all_cols_set = {col for row in rows for col in row}
    tie_cols = {}    # sec_key -> col name for "If tie" question
    winner_cols = {} # sec_key -> col name for "What version" question
    for col in all_cols_set:
        for sec_key, pat in SECTION_PATTERNS:
            col_lower = col.lower()
            sec_match = pat.search(col)
            if not sec_match:
                continue
            if 'iftie' in col_lower:
                tie_cols[sec_key] = col
            elif 'whatversion' in col_lower:
                winner_cols[sec_key] = col
            break

    result = []
    for sec_key, label in SECTION_LABELS.items():
        v1_cols_list = v_cols[sec_key][1]
        v2_cols_list = v_cols[sec_key][2]
        if not v1_cols_list or not v2_cols_list:
            continue

        v1_scores, v2_scores, v1w, v2w, ties = [], [], 0, 0, 0
        tie_both_correct = 0
        tie_both_incorrect = 0
        for row in ab_rows:
            vals1 = [coerce_float(row.get(c)) for c in v1_cols_list]
            vals1 = [v for v in vals1 if v is not None]
            vals2 = [coerce_float(row.get(c)) for c in v2_cols_list]
            vals2 = [v for v in vals2 if v is not None]
            if not vals1 or not vals2:
                continue
            s1 = safe_avg(vals1)
            s2 = safe_avg(vals2)
            v1_scores.append(s1)
            v2_scores.append(s2)

            # Use the explicit "what version" column if available
            winner_col = winner_cols.get(sec_key)
            if winner_col and row.get(winner_col):
                winner_val = str(row[winner_col]).strip().lower()
                if '1' in winner_val:
                    v1w += 1
                elif '2' in winner_val:
                    v2w += 1
                else:
                    ties += 1
            else:
                if abs(s1 - s2) < 5:
                    ties += 1
                elif s1 > s2:
                    v1w += 1
                else:
                    v2w += 1

            # Tie type from explicit column
            tie_col = tie_cols.get(sec_key)
            if tie_col and row.get(tie_col):
                tie_val = str(row[tie_col]).strip().lower()
                if 'correct' in tie_val and 'incorrect' not in tie_val:
                    tie_both_correct += 1
                elif 'incorrect' in tie_val:
                    tie_both_incorrect += 1

        if not v1_scores:
            continue

        v1r = round(safe_avg(v1_scores))
        v2r = round(safe_avg(v2_scores))
        tie_type = 'Both Correct' if tie_both_correct >= tie_both_incorrect else 'Both Incorrect'

        result.append({
            'section':  label,
            'v1':       v1r,
            'v2':       v2r,
            'v1wins':   v1w,
            'v2wins':   v2w,
            'ties':     ties,
            'tieType':  tie_type,
            'winner':   'V1' if v1w >= v2w else 'V2',
        })

    return result


def compute_trend_data(rows, status_col, date_col):
    """Group rows by month and compute pass rate per month."""
    if not status_col or not date_col:
        return [], []

    monthly = defaultdict(list)
    for row in rows:
        dt  = str(row.get(date_col, '') or '')
        st  = str(row.get(status_col, '') or '').strip().lower()
        month = dt[:7]  # "YYYY-MM"
        if len(month) == 7:
            monthly[month].append(1 if st == 'pass' else 0)

    months  = sorted(monthly)
    scores  = [round(sum(monthly[m]) / len(monthly[m]) * 100) for m in months]

    # Format months as short names for the chart
    MONTH_NAMES = {
        '01': 'Jan', '02': 'Feb', '03': 'Mar', '04': 'Apr',
        '05': 'May', '06': 'Jun', '07': 'Jul', '08': 'Aug',
        '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dec',
    }
    labels = [MONTH_NAMES.get(m[5:7], m) for m in months]
    return labels, scores


def compute_pathway_counts(rows, version_col):
    """Count rows by selected pathway using the versions-count column.

    A value of 1 means single-version pathway, 2 means A/B pathway.
    """
    counts = {
        'total': len(rows),
        'singleVersion': 0,
        'abVersion': 0,
        'unknown': 0,
    }
    if not version_col:
        counts['unknown'] = len(rows)
        return counts

    for row in rows:
        v = parse_version_choice(row.get(version_col))
        if v == 1:
            counts['singleVersion'] += 1
        elif v == 2:
            counts['abVersion'] += 1
        else:
            counts['unknown'] += 1

    return counts


def normalize_rows(raw_rows):
    cleaned = []
    for row in raw_rows:
        entry = {}
        for k, v in row.items():
            if k in ('etag', 'odata.etag'):
                continue
            if isinstance(v, (bytes, bytearray)):
                try:
                    v = v.decode()
                except Exception:
                    v = str(v)
            entry[k] = v
        cleaned.append(entry)
    return cleaned


def _first_present(row, keys, default='Unknown'):
    for key in keys:
        val = row.get(key)
        if val is None:
            continue
        text = str(val).strip()
        if text:
            return text
    return default


def compute_encounter_summaries(rows, col_map, status_col=None, specialty_col=None):
    """Build encounter-level summaries from Azure table rows."""
    summaries = []

    for row in rows:
        section_scores = defaultdict(list)
        for col, meta in col_map.items():
            if meta['version'] is not None:
                continue
            val = coerce_float(row.get(col))
            if val is None:
                continue
            section_scores[meta['section']].append(val)

        sec_avg = {
            section: safe_avg(values)
            for section, values in section_scores.items()
            if values
        }

        hpi = sec_avg.get('hpi')
        ap = sec_avg.get('ap')
        pe = sec_avg.get('physical_exam')
        overall_values = [v for v in [hpi, ap, pe] if v is not None]
        overall = safe_avg(overall_values)

        status_raw = str(row.get(status_col, '')).strip() if status_col else ''
        if status_raw:
            status = status_raw
        else:
            if overall is None:
                status = 'Unknown'
            elif overall >= 85:
                status = 'Pass'
            elif overall >= 70:
                status = 'At Risk'
            else:
                status = 'Fail'

        summaries.append({
            'id': _first_present(row, ['NEATEncounterID', 'Id', 'RowKey', 'id'], default='Unknown'),
            'specialty': _first_present(
                row,
                [specialty_col, 'NEATSpecialty', 'Specialty', 'specialty', 'Department', 'dept'] if specialty_col else ['NEATSpecialty', 'Specialty', 'specialty', 'Department', 'dept'],
                default='Unknown',
            ),
            'region': _first_present(row, ['SelectRegion', 'Region', 'region'], default='Unknown'),
            'evaluator': _first_present(row, ['CIUserName', 'Name', 'Email', 'evaluator'], default='Unknown'),
            'hpi': hpi if hpi is not None else 0,
            'ap': ap if ap is not None else 0,
            'pe': pe if pe is not None else 0,
            'overall': overall if overall is not None else 0,
            'status': status,
        })

    return summaries


def transform_rows(raw_rows):
    rows = normalize_rows(raw_rows)
    if not rows:
        return {
            'encounters':      [],
            'sectionData':     {},
            'versionData':     [],
            'trendMonths':     [],
            'trendScores':     [],
            'specialtyHeatmap': {},
        }

    all_cols    = {col for row in rows for col in row} - SYSTEM_COLS
    col_map     = classify_columns(all_cols)

    # Detect helper columns
    version_col   = find_version_column(all_cols)
    status_col    = find_col(all_cols, re.compile(r'\bstatus\b',                        re.I))
    specialty_col = find_col(all_cols, re.compile(r'specialty|speciali[st]|dept',       re.I))
    date_col      = find_col(all_cols, re.compile(r'\bdate\b|\btimestamp\b',            re.I))

    section_data     = compute_section_data(rows, col_map)
    version_data     = compute_version_data(rows, col_map, version_col)
    trend_months, trend_scores = compute_trend_data(rows, status_col, date_col)
    specialty_heatmap = compute_specialty_heatmap(rows, col_map, specialty_col)
    pathway_counts = compute_pathway_counts(rows, version_col)
    encounter_summaries = compute_encounter_summaries(rows, col_map, status_col=status_col, specialty_col=specialty_col)

    return {
        'encounters':       encounter_summaries,
        'sectionData':      section_data,
        'versionData':      version_data,
        'trendMonths':      trend_months,
        'trendScores':      trend_scores,
        'specialtyHeatmap': specialty_heatmap,
        'pathwayCounts':    pathway_counts,
    }


@app.route('/api/GetFormData')
def get_form_data():
    try:
        tbl = _ensure_client()
    except RuntimeError as exc:
        # return a JSON error instead of letting import-time failure bubble
        return jsonify({'error': str(exc)}), 500
    except Exception as exc:
        return jsonify({'error': f'Client initialization failed: {exc}'}), 500

    try:
        entities = list(tbl.list_entities())
        return jsonify(transform_rows(entities))
    except Exception as exc:
        return jsonify({'error': f'Azure query failed: {exc}'}), 502


@app.route('/api/schema')
def get_schema():
    """Debug: returns all column names and 2 sample rows. Use to confirm column mapping."""
    try:
        tbl = _ensure_client()
    except RuntimeError as exc:
        return jsonify({'error': str(exc)}), 500
    except Exception as exc:
        return jsonify({'error': f'Client initialization failed: {exc}'}), 500

    try:
        entities = list(tbl.list_entities(results_per_page=10))[:10]
        rows = normalize_rows(entities)
        cols = sorted({k for r in rows for k in r if k not in SYSTEM_COLS})
        all_cols = {k for r in rows for k in r} - SYSTEM_COLS
        col_map = classify_columns(all_cols)
        return jsonify({
            'total_rows_sampled': len(rows),
            'columns': cols,
            'column_mappings': {c: m for c, m in col_map.items()},
            'sample_rows': rows[:2],
        })
    except Exception as exc:
        return jsonify({'error': f'Azure schema query failed: {exc}'}), 502


@app.route('/health')
def health():
    return jsonify({'ok': True}), 200


@app.route('/', defaults={'path': 'clinical_eval_dashboard.html'})
@app.route('/<path:path>')
def serve_static(path):
    target = path or 'clinical_eval_dashboard.html'
    full_path = os.path.join(BASE_DIR, target)
    if os.path.exists(full_path):
        return send_from_directory(BASE_DIR, target)

    index_path = os.path.join(BASE_DIR, 'index.html')
    if os.path.exists(index_path):
        return send_from_directory(BASE_DIR, 'index.html')

    return jsonify({'error': f'File not found: {target}'}), 404


if __name__ == '__main__':
    port = int(os.getenv('PORT', 5001))
    app.run(host='0.0.0.0', port=port, debug=True, use_reloader=False)
