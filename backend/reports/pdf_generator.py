"""
LegacyBridge AI — Enterprise PDF Report Generator
Pure canvas-based rendering for pixel-perfect layout.
Replace your existing pdf_generator.py with this file entirely.
"""

from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib.colors import HexColor
import io

# ─── COLORS ──────────────────────────────────────────────────────────────────
NAVY       = HexColor('#0B1929')
NAVY2      = HexColor('#1E3A5F')
CYAN       = HexColor('#0891B2')
WHITE      = HexColor('#FFFFFF')
OFF_WHITE  = HexColor('#F8FAFC')
BORDER     = HexColor('#E2E8F0')
TEXT       = HexColor('#0F172A')
TEXT_MID   = HexColor('#475569')
TEXT_MUTED = HexColor('#94A3B8')
NAVY_MUTED = HexColor('#4A6B8A')

RED        = HexColor('#DC2626')
RED_BG     = HexColor('#FEF2F2')
RED_BD     = HexColor('#FECACA')
AMBER      = HexColor('#D97706')
AMBER_BG   = HexColor('#FFFBEB')
AMBER_BD   = HexColor('#FDE68A')
GREEN      = HexColor('#059669')
GREEN_BG   = HexColor('#ECFDF5')
GREEN_BD   = HexColor('#A7F3D0')
BLUE       = HexColor('#1D4ED8')
BLUE_BG    = HexColor('#EFF6FF')
BLUE_BD    = HexColor('#BFDBFE')
PURPLE     = HexColor('#7C3AED')
CYAN_BG    = HexColor('#F0F9FF')
CYAN_BD    = HexColor('#BAE6FD')

# ─── PAGE CONSTANTS ───────────────────────────────────────────────────────────
W, H   = letter   # 612 x 792 points
LM     = 36       # left margin
RM     = 576      # right edge
CW     = 540      # content width
HEADER_H = 58     # header bar height — tall enough for two text lines
FOOTER_H = 30     # footer bar height
TOP_Y    = H - HEADER_H - 10   # content start y


# ─── COLOR HELPERS ────────────────────────────────────────────────────────────
def sev_colors(sev):
    """Returns (foreground, background, border) for a severity level."""
    return {
        'CRITICAL': (RED,       RED_BG,   RED_BD),
        'WARNING':  (AMBER,     AMBER_BG, AMBER_BD),
        'HEALTHY':  (GREEN,     GREEN_BG, GREEN_BD),
        'INFO':     (BLUE,      BLUE_BG,  BLUE_BD),
        'MATCH':    (GREEN,     GREEN_BG, GREEN_BD),
        'MISMATCH': (RED,       RED_BG,   RED_BD),
    }.get(sev, (TEXT_MUTED, OFF_WHITE, BORDER))


def pri_color(p):
    return {'P1': RED, 'P2': AMBER, 'P3': BLUE}.get(p, TEXT_MUTED)


def rc_color(rc):
    return {
        'CDC_SCHEMA_DRIFT':     RED,
        'CDC_TRIGGER_GAP':      RED,
        'SOFT_DELETE_MISMATCH': RED,
        'TYPE_COERCION':        AMBER,
        'TZ_MISMATCH':          AMBER,
        'NULL_EMPTY_MISMATCH':  BLUE,
        'HEALTHY':              GREEN,
    }.get(rc, TEXT_MUTED)


# ─── DRAWING PRIMITIVES ───────────────────────────────────────────────────────
def box(c, x, y, w, h, fill, stroke=None, sw=0.5):
    """Draw a filled rectangle with optional stroke border."""
    c.setFillColor(fill)
    c.rect(x, y, w, h, fill=1, stroke=0)
    if stroke:
        c.setStrokeColor(stroke)
        c.setLineWidth(sw)
        c.rect(x, y, w, h, fill=0, stroke=1)


def txt(c, x, y, text, font='Helvetica', size=9,
        color=TEXT, align='left'):
    """Draw a single line of text."""
    c.setFillColor(color)
    c.setFont(font, size)
    s = str(text)
    if align == 'right':
        c.drawRightString(x, y, s)
    elif align == 'center':
        c.drawCentredString(x, y, s)
    else:
        c.drawString(x, y, s)


def wrap_text(c, x, y, text, max_w,
              font='Helvetica', size=8.5,
              color=TEXT_MID, lh=12):
    """
    Draw wrapped text. Returns the y position after the last line.
    """
    c.setFillColor(color)
    c.setFont(font, size)
    words = str(text).split()
    line  = ''
    cy    = y
    for word in words:
        test = (line + ' ' + word).strip()
        if c.stringWidth(test, font, size) <= max_w:
            line = test
        else:
            if line:
                c.drawString(x, cy, line)
                cy -= lh
            line = word
    if line:
        c.drawString(x, cy, line)
    return cy - lh


def vline(c, x, y_bottom, y_top, color=BORDER, lw=0.3):
    """Draw a vertical line."""
    c.setStrokeColor(color)
    c.setLineWidth(lw)
    c.line(x, y_bottom, x, y_top)


def hline(c, x, y, w, color=BORDER, lw=0.5):
    """Draw a horizontal line."""
    c.setStrokeColor(color)
    c.setLineWidth(lw)
    c.line(x, y, x + w, y)


# ─── PAGE CHROME ──────────────────────────────────────────────────────────────
def draw_header(c, run_id, generated):
    """
    Draw the navy header bar at the top of the page.
    Left:  LegacyBridge AI  |  Data Migration Incident Report
    Right: Run ID / Date (stacked, right-aligned)
    """
    # Navy background
    box(c, 0, H - HEADER_H, W, HEADER_H, NAVY)
    # Cyan top accent line (2px)
    box(c, 0, H - 2, W, 2, CYAN)
    # Bottom border
    box(c, 0, H - HEADER_H - 1, W, 1, NAVY2)

    # ── LEFT SIDE — two lines clearly stacked ────────────────
    # Line 1: "LegacyBridge AI" bold white — upper line
    txt(c, LM, H - 22, 'LegacyBridge AI',
        'Helvetica-Bold', 13, WHITE)
    # Line 2: subtitle muted — lower line, clear gap below line 1
    txt(c, LM, H - 40,
        'Data Migration Incident Report',
        size=8.5, color=NAVY_MUTED)

    # ── RIGHT SIDE — two lines clearly stacked, right-aligned ─
    # Line 1: Run ID bold white — upper line
    txt(c, RM, H - 22,
        f'Run ID:  {run_id}',
        'Helvetica-Bold', 8, WHITE, 'right')
    # Line 2: date muted — lower line
    txt(c, RM, H - 40,
        str(generated),
        size=7.5, color=NAVY_MUTED, align='right')


def draw_footer(c, page_num):
    """
    Draw the navy footer bar at the bottom of the page.
    Left:  Confidential notice
    Right: Page number
    """
    # Navy background
    box(c, 0, 0, W, FOOTER_H, NAVY)
    # Top border
    box(c, 0, FOOTER_H, W, 1, NAVY2)
    # Cyan left accent
    box(c, 0, 0, 3, FOOTER_H, CYAN)

    txt(c, 12, 11,
        'CONFIDENTIAL  —  LegacyBridge AI  |  '
        'AI-Powered Data Migration Intelligence',
        size=7, color=NAVY_MUTED)
    txt(c, RM, 11, f'Page {page_num}',
        'Helvetica-Bold', 8, NAVY_MUTED, 'right')


# ─── SECTION HEADER ───────────────────────────────────────────────────────────
def section_header(c, y, title, subtitle=''):
    """
    Draw a section header bar.
    Returns the new y position below the bar.
    """
    bar_h = 30
    # Visible background — distinct from white page
    box(c, LM, y - bar_h, CW, bar_h,
        HexColor('#E2EAF4'), HexColor('#B0C4DE'), 1)
    # Thick cyan left accent (5px)
    box(c, LM, y - bar_h, 5, bar_h, CYAN)
    # Strong bottom border
    box(c, LM, y - bar_h - 1, CW, 2, CYAN)
    # Title — navy bold
    txt(c, LM + 16, y - bar_h + 11,
        title, 'Helvetica-Bold', 11, NAVY)
    # Subtitle — right side muted
    if subtitle:
        txt(c, RM, y - bar_h + 11,
            subtitle, size=8, color=TEXT_MUTED, align='right')
    return y - bar_h - 14   # generous gap below bar


# ─── TABLE HELPERS ────────────────────────────────────────────────────────────
def table_header_row(c, y, columns, row_h=22):
    """
    Draw a dark navy table header row.
    columns = list of (label, width, align)
    All widths must sum to CW (540).
    """
    # Navy background
    box(c, LM, y - row_h, CW, row_h, NAVY)
    # Cyan underline
    box(c, LM, y - row_h, CW, 2, CYAN)
    # Column labels
    x = LM
    for label, w, align in columns:
        lx = x + (w / 2 if align == 'center' else 9)
        txt(c, lx, y - row_h + 8,
            label, 'Helvetica-Bold', 7.5, WHITE,
            'center' if align == 'center' else 'left')
        x += w
    return y - row_h


def table_data_row(c, y, cells, col_widths, row_h=20,
                   alternate=False, highlight=False):
    """
    Draw a single table data row.
    cells      = list of (text, align, color, font, size)
    col_widths = list of widths (must sum to CW)
    """
    bg = RED_BG if highlight else (OFF_WHITE if alternate else WHITE)
    box(c, LM, y - row_h, CW, row_h, bg, BORDER, 0.5)

    x = LM
    for i, ((val, align, color, font, size), w) in enumerate(
            zip(cells, col_widths)):
        lx = x + (w / 2 if align == 'center'
                   else w - 9 if align == 'right' else 9)
        txt(c, lx, y - row_h + 7, val, font, size, color, align)
        # Column divider
        if i < len(cells) - 1:
            vline(c, x + w, y - row_h, y)
        x += w
    return y - row_h


# ─── TABLE NAME BAR ───────────────────────────────────────────────────────────
def table_name_bar(c, y, name, severity, bar_h=28):
    """
    Draw a per-table colored name bar above its schema drift table.
    Returns new y position.
    """
    fc, bg, bd = sev_colors(severity)
    box(c, LM, y - bar_h, CW, bar_h, bg, bd, 0.8)
    box(c, LM, y - bar_h, 4, bar_h, fc)    # left accent
    # Bottom border — separates bar from table below
    box(c, LM, y - bar_h, CW, 1, bd)
    txt(c, LM + 12, y - bar_h + 10,
        name, 'Helvetica-Bold', 10, TEXT)
    txt(c, RM, y - bar_h + 10,
        severity, 'Helvetica-Bold', 9, fc, 'right')
    return y - bar_h


# ─── FINDING CARD ─────────────────────────────────────────────────────────────
def finding_card(c, y, number, table_name, root_cause,
                 priority, confidence, affected_rows, fix_text):
    """
    Draw a two-part RCA finding card.
    Part A: coloured header strip with finding metadata.
    Part B: white detail strip with affected rows + fix.
    Returns new y position after the card.
    """
    fc  = rc_color(root_cause)
    pfc = pri_color(priority)

    is_critical = root_cause in (
        'CDC_SCHEMA_DRIFT', 'CDC_TRIGGER_GAP', 'SOFT_DELETE_MISMATCH')
    is_warning  = root_cause in ('TYPE_COERCION', 'TZ_MISMATCH')
    is_healthy  = root_cause == 'HEALTHY'

    sev = ('CRITICAL' if is_critical else
           'WARNING'  if is_warning  else
           'HEALTHY'  if is_healthy  else 'INFO')
    _, bg, bd = sev_colors(sev)

    # ── PART A — header strip ─────────────────────────────────
    ah = 30
    box(c, LM, y - ah, CW, ah, bg, bd, 0.8)
    box(c, LM, y - ah, 4, ah, fc)   # left accent

    txt(c, LM + 12, y - ah + 10,
        f'#{number:02d}', 'Helvetica-Bold', 9, TEXT_MUTED)
    txt(c, LM + 44, y - ah + 10,
        table_name, 'Helvetica-Bold', 10, TEXT)
    txt(c, LM + 164, y - ah + 10,
        root_cause, 'Courier', 9, fc)
    txt(c, RM - 64, y - ah + 10,
        priority, 'Helvetica-Bold', 11, pfc, 'right')
    txt(c, RM, y - ah + 10,
        confidence, 'Helvetica-Bold', 11, TEXT, 'right')

    # ── PART B — detail strip ────────────────────────────────
    bh = 58
    box(c, LM, y - ah - bh, CW, bh, WHITE, bd, 0.8)
    box(c, LM, y - ah - bh, 4, bh, fc)   # left accent
    vline(c, LM + 152, y - ah - bh, y - ah)  # column divider

    # Left — affected rows
    txt(c, LM + 12, y - ah - 14,
        'AFFECTED ROWS', 'Helvetica-Bold', 7, TEXT_MUTED)
    afc = fc if affected_rows > 0 else GREEN
    txt(c, LM + 12, y - ah - 38,
        f'{affected_rows:,}', 'Helvetica-Bold', 20, afc)

    # Right — recommended fix
    txt(c, LM + 162, y - ah - 14,
        'RECOMMENDED FIX', 'Helvetica-Bold', 7, TEXT_MUTED)
    wrap_text(c, LM + 162, y - ah - 28,
              fix_text, 368, size=8.5, color=TEXT_MID, lh=12)

    return y - ah - bh - 10   # gap between cards


# ─── GOVERNANCE BOX ───────────────────────────────────────────────────────────
def governance_box(c, y):
    """Draw the AI governance notice box. Returns new y."""
    gh = 62
    box(c, LM, y - gh, CW, gh, CYAN_BG, CYAN_BD, 1)
    box(c, LM, y - gh, 3, gh, CYAN)
    txt(c, LM + 12, y - 16,
        'AI GOVERNANCE NOTICE', 'Helvetica-Bold', 9,
        HexColor('#0C4A6E'))
    wrap_text(
        c, LM + 12, y - 30,
        'All agent reasoning steps are fully logged and auditable. '
        'This system investigates and recommends only — it never '
        'remediates automatically. Human approval is required before '
        'applying any fix. Before production deployment review '
        'EU AI Act risk classification, NIST AI RMF controls, and your '
        "organization's data residency and access control policies.",
        516, size=8, color=HexColor('#0E7490'), lh=11)
    return y - gh


# ─── MAIN GENERATOR ───────────────────────────────────────────────────────────
def generate_report(report_data: dict) -> bytes:
    """
    Generate the enterprise PDF report.

    report_data keys:
        run_id        str
        generated     str
        health_score  int
        critical      int
        warnings      int
        tables_count  int
        duration      str
        root_causes   int
        schema        list  — see schema_data format
        recon         list  — see recon_data format
        cdc           list  — see cdc_data format
        cdc_patterns  list  — see cdc_patterns format
        findings      list  — see findings format

    schema item:
        (table_name, severity, issues_list_or_None)
        issues item: (issue_type, column, source_type, target_type, severity)
        None   = data-level only (supplier_contract message)
        []     = healthy / no issues

    recon item:
        (table_name, source, target, delta, delta_pct_str,
         checksum_bool, status_str)

    cdc item:
        (table_name, total, captured, missed, gap_rate_str, risk_str)

    cdc_patterns item:
        (pattern_name, count_str, description)

    findings item:
        (table_name, root_cause, priority, confidence_str,
         affected_rows_int, fix_text)
    """

    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)

    run_id    = report_data.get('run_id', 'N/A')
    generated = report_data.get('generated', 'N/A')
    page_num  = [1]

    # ── helpers ──────────────────────────────────────────────
    def start_page():
        draw_header(c, run_id, generated)
        draw_footer(c, page_num[0])

    def next_page():
        c.showPage()
        page_num[0] += 1
        start_page()

    def check_space(y, needed):
        """Start a new page if not enough vertical space."""
        if y - needed < FOOTER_H + 10:
            next_page()
            return TOP_Y
        return y

    # ═══════════════════════════════════════════════════════════
    # PAGE 1 — EXECUTIVE SUMMARY
    # ═══════════════════════════════════════════════════════════
    start_page()
    y = TOP_Y

    y = section_header(c, y, 'EXECUTIVE SUMMARY',
                       'Migration health overview and key metrics')

    score  = report_data.get('health_score', 0)
    s_fc   = (RED   if score < 20 else
              AMBER if score < 60 else GREEN)
    s_bg   = (RED_BG   if score < 20 else
              AMBER_BG if score < 60 else GREEN_BG)
    s_bd   = (RED_BD   if score < 20 else
              AMBER_BD if score < 60 else GREEN_BD)
    s_lbl  = ('CRITICAL' if score < 20 else
              'AT RISK'  if score < 60 else 'HEALTHY')

    # Health score card
    card_h = 96
    box(c, LM, y - card_h, CW, card_h, s_bg, s_bd, 1.2)
    box(c, LM, y - card_h, 5, card_h, s_fc)   # left accent

    # Label — top of card
    txt(c, LM + 16, y - 13,
        'MIGRATION HEALTH SCORE', 'Helvetica-Bold', 7, TEXT_MUTED)

    # Big score
    txt(c, LM + 16, y - 62,
        str(score), 'Helvetica-Bold', 56, s_fc)
    # "/ 100" fixed position
    txt(c, LM + 100, y - 46,
        '/ 100', size=17, color=TEXT_MUTED)

    # Severity pill — right side, clearly separated from score
    pill_w, pill_h = 100, 30
    pill_x = RM - pill_w
    pill_y = y - 38   # vertically centered in upper half of card
    box(c, pill_x, pill_y - pill_h, pill_w, pill_h, s_fc)
    txt(c, pill_x + pill_w / 2, pill_y - pill_h + 10,
        s_lbl, 'Helvetica-Bold', 12, WHITE, 'center')

    y -= card_h + 10

    # 5 metric cards — stronger visual
    metrics = [
        (report_data.get('duration',     '—'),  'DURATION',    CYAN),
        (report_data.get('critical',      0),   'CRITICAL',    RED),
        (report_data.get('warnings',      0),   'WARNINGS',    AMBER),
        (report_data.get('tables_count',  0),   'TABLES',      NAVY),
        (report_data.get('root_causes',   0),   'ROOT CAUSES', PURPLE),
    ]
    mw  = 104
    gap = 4
    mh  = 70
    for i, (val, label, col) in enumerate(metrics):
        mx = LM + i * (mw + gap)
        # Card background with border
        box(c, mx, y - mh, mw, mh, WHITE, BORDER, 0.8)
        # Top colour strip — 4px
        box(c, mx, y - 4,  mw, 4, col)
        # Bottom colour strip — subtle
        box(c, mx, y - mh, mw, 2, col)
        # Large number
        txt(c, mx + mw / 2, y - 32,
            str(val), 'Helvetica-Bold', 24, col, 'center')
        # Label
        txt(c, mx + mw / 2, y - 56,
            label, 'Helvetica-Bold', 7, TEXT_MUTED, 'center')
    y -= mh + 12

    # Recommendation banner — dynamic based on severity
    rec_texts = {
        'CRITICAL': 'Immediate attention required. Critical data integrity issues detected. Do not proceed with go-live until all P1 issues are resolved.',
        'AT RISK': 'Significant issues detected. Address all P1 and P2 findings before proceeding with go-live.',
        'HEALTHY': 'Migration appears healthy. Review any warnings before final sign-off.',
    }
    rec_colors = {
        'CRITICAL': (RED, RED_BG, RED_BD, HexColor('#7F1D1D')),
        'AT RISK': (AMBER, AMBER_BG, AMBER_BD, HexColor('#78350F')),
        'HEALTHY': (GREEN, GREEN_BG, GREEN_BD, HexColor('#064E3B')),
    }
    r_accent, r_bg, r_bd, r_txt = rec_colors.get(s_lbl, rec_colors['CRITICAL'])
    rec_h = 52
    box(c, LM, y - rec_h, CW, rec_h, r_bg, r_bd, 1)
    box(c, LM, y - rec_h, 4,  rec_h, r_accent)
    txt(c, LM + 14, y - 15,
        'RECOMMENDATION:', 'Helvetica-Bold', 9, r_accent)
    wrap_text(c, LM + 14, y - 29,
        rec_texts.get(s_lbl, rec_texts['CRITICAL']),
        CW - 28, font='Helvetica', size=8.5,
        color=r_txt, lh=13)
    y -= rec_h

    # ═══════════════════════════════════════════════════════════
    # PAGE 2 — SCHEMA DRIFT
    # ═══════════════════════════════════════════════════════════
    next_page()
    y = TOP_Y

    y = section_header(c, y, 'SCHEMA & DATA DRIFT ANALYSIS',
                       'Structural differences between source and target databases')

    schema_items = report_data.get('schema', [])
    schema_cols  = [
        ('ISSUE TYPE',  110, 'left'),
        ('COLUMN',      110, 'left'),
        ('SOURCE TYPE', 130, 'left'),
        ('TARGET TYPE', 130, 'left'),
        ('SEVERITY',     60, 'center'),
    ]  # widths sum = 540

    for tname, tsev, issues in schema_items:
        # Gap before each table group — breathing room
        y -= 6
        # Table name bar
        needed = 26 + (0 if not issues else len(issues) * 20 + 22) + 16
        y = check_space(y, needed)
        y = table_name_bar(c, y, tname, tsev)

        if issues is None:
            # Data-level mismatch only
            bh = 24
            y  = check_space(y, bh)
            box(c, LM, y - bh, CW, bh, WHITE, BORDER, 0.5)
            txt(c, LM + 12, y - bh + 8,
                'Schema matches. Data-level NULL vs empty string '
                'divergence detected in row checksums.',
                size=8.5, color=TEXT_MID)
            y -= bh

        elif len(issues) == 0:
            bh = 24
            y  = check_space(y, bh)
            box(c, LM, y - bh, CW, bh, WHITE, BORDER, 0.5)
            txt(c, LM + 12, y - bh + 8,
                'No schema differences detected.',
                size=8.5, color=GREEN)
            y -= bh

        else:
            y = check_space(y, 22 + len(issues) * 20)
            y = table_header_row(c, y, schema_cols)
            for idx, (itype, col_, src, tgt, sev) in enumerate(issues):
                sfc, _, _ = sev_colors(sev)
                hl = (sev == 'CRITICAL')
                y  = check_space(y, 20)
                cells = [
                    (itype, 'left',   HexColor('#1E40AF'), 'Courier',        8),
                    (col_,  'left',   TEXT,                'Helvetica-Bold', 9),
                    (src,   'left',   TEXT_MID,            'Helvetica',      8),
                    (tgt,   'left',   TEXT_MID,            'Helvetica',      8),
                    (sev,   'center', sfc,                 'Helvetica-Bold', 8),
                ]
                y = table_data_row(
                    c, y, cells,
                    [110, 110, 130, 130, 60],
                    alternate=(idx % 2 == 1),
                    highlight=hl)
        y -= 14   # clear gap between table groups

    # ═══════════════════════════════════════════════════════════
    # PAGE 3 — ROW RECONCILIATION + CDC
    # ═══════════════════════════════════════════════════════════
    next_page()
    y = TOP_Y

    # ── Row Reconciliation ────────────────────────────────────
    y = section_header(c, y, 'ROW RECONCILIATION',
                       'Row count and checksum validation across all tables')

    r_cols = [
        ('TABLE',     130, 'left'),
        ('SOURCE',     70, 'right'),
        ('TARGET',     70, 'right'),
        ('DELTA',      55, 'right'),
        ('DELTA %',    55, 'center'),
        ('CHECKSUM',   70, 'center'),
        ('STATUS',     90, 'center'),
    ]  # 130+70+70+55+55+70+90 = 540

    y = table_header_row(c, y, r_cols)
    for idx, (tn, src, tgt, delta, dpct, chk, status) in enumerate(
            report_data.get('recon', [])):
        sfc = RED   if status == 'MISMATCH' else GREEN
        cfc = GREEN if chk   else RED
        hl  = (status == 'MISMATCH')
        cells = [
            (tn,           'left',   TEXT,  'Helvetica-Bold', 9),
            (f'{src:,}',   'right',  TEXT,  'Helvetica',      9),
            (f'{tgt:,}',   'right',  TEXT,  'Helvetica',      9),
            (f'{delta:,}', 'right',
             RED if delta > 0 else GREEN, 'Helvetica-Bold', 9),
            (dpct,         'center', TEXT_MID, 'Helvetica',   9),
            ('✓ Yes' if chk else '✗ No',
             'center', cfc, 'Helvetica-Bold', 9),
            (status, 'center', sfc, 'Helvetica-Bold', 9),
        ]
        y = check_space(y, 20)
        y = table_data_row(c, y, cells, [130,70,70,55,55,70,90],
                           alternate=(idx % 2 == 1), highlight=hl)

    y -= 16

    # ── CDC Analysis ──────────────────────────────────────────
    y = check_space(y, 60)
    y = section_header(c, y, 'CDC EVENT ANALYSIS',
                       'Change Data Capture gap detection and pattern analysis')

    c_cols = [
        ('TABLE',        130, 'left'),
        ('TOTAL EVENTS',  90, 'right'),
        ('CAPTURED',      80, 'right'),
        ('MISSED',        75, 'right'),
        ('GAP RATE',      80, 'center'),
        ('RISK',          85, 'center'),
    ]  # 130+90+80+75+80+85 = 540

    y = table_header_row(c, y, c_cols)
    for idx, (tn, total, cap, missed, gap, risk) in enumerate(
            report_data.get('cdc', [])):
        gv  = float(str(gap).replace('%', '') or 0)
        gfc = RED if gv > 5 else (AMBER if gv > 0 else GREEN)
        rfc = RED if risk == 'HIGH' else GREEN
        hl  = (missed > 0)
        cells = [
            (tn,            'left',   TEXT,  'Helvetica-Bold', 9),
            (f'{total:,}',  'right',  TEXT,  'Helvetica',      9),
            (f'{cap:,}',    'right',  TEXT,  'Helvetica',      9),
            (f'{missed:,}', 'right',
             RED if missed > 0 else GREEN, 'Helvetica-Bold', 9),
            (gap,           'center', gfc,   'Helvetica-Bold', 9),
            (risk,          'center', rfc,   'Helvetica-Bold', 9),
        ]
        y = check_space(y, 20)
        y = table_data_row(c, y, cells, [130,90,80,75,80,85],
                           alternate=(idx % 2 == 1), highlight=hl)

    # CDC gap patterns — proper section header
    patterns = report_data.get('cdc_patterns', [])
    if patterns:
        y -= 14
        y  = check_space(y, 44 + len(patterns) * 22)
        y  = section_header(c, y, 'CDC GAP PATTERN DETAIL',
                            'Vendor table — missed event breakdown')
        gp_cols = [
            ('PATTERN',     160, 'left'),
            ('COUNT',        60, 'center'),
            ('DESCRIPTION', 320, 'left'),
        ]
        y = table_header_row(c, y, gp_cols)
        for idx, (pat, cnt, desc) in enumerate(patterns):
            rh = 22
            bg = AMBER_BG if idx % 2 == 0 else WHITE
            box(c, LM, y - rh, CW, rh, bg, BORDER, 0.5)
            x = LM
            for (val, align, color, font, size), w in zip([
                (pat,  'left',   HexColor('#1E40AF'), 'Courier',        8),
                (cnt,  'center', TEXT,                'Helvetica-Bold', 9),
                (desc, 'left',   TEXT_MID,            'Helvetica',      8.5),
            ], [160, 60, 320]):
                lx = x + (w / 2 if align == 'center' else 10)
                txt(c, lx, y - rh + 8, val, font, size, color,
                    'center' if align == 'center' else 'left')
                vline(c, x + w, y - rh, y)
                x += w
            y -= rh

    # ═══════════════════════════════════════════════════════════
    # PAGE 4+ — RCA FINDINGS
    # ═══════════════════════════════════════════════════════════
    next_page()
    y = TOP_Y

    y = section_header(c, y, 'AI ROOT CAUSE ANALYSIS',
                       'Autonomous agent investigation — confidence-scored findings')

    findings_data = report_data.get('findings', [])
    p_count = len([f for f in findings_data if f[1] != 'HEALTHY'])
    txt(c, LM, y - 4,
        f'The AI agent identified {p_count} root causes '
        f'across {report_data.get("tables_count", 5)} investigated tables.',
        size=8.5, color=TEXT_MID)
    y -= 18

    for i, (tn, rc, pri, conf, aff, fix) in enumerate(findings_data, 1):
        y = check_space(y, 100)
        y = finding_card(c, y, i, tn, rc, pri, conf, aff, fix)

    # ═══════════════════════════════════════════════════════════
    # LAST PAGE — RECOMMENDED FIXES
    # ═══════════════════════════════════════════════════════════
    next_page()
    y = TOP_Y

    y = section_header(c, y, 'RECOMMENDED FIXES',
                       'Prioritized remediation plan — resolve all P1 before go-live')
    txt(c, LM, y - 4,
        'P1 issues must be resolved before proceeding with go-live. '
        'P2 within the next sprint. P3 are low risk.',
        size=8.5, color=TEXT_MID)
    y -= 18

    fix_cols = [
        ('#',             28,  'center'),
        ('PRI',           40,  'center'),
        ('TABLE',        110,  'left'),
        ('ROOT CAUSE',   130,  'left'),
        ('RECOMMENDED ACTION', 232, 'left'),
    ]  # 28+40+110+130+232 = 540

    y = table_header_row(c, y, fix_cols)

    fix_num = 0
    for i, (tn, rc, pri, conf, aff, fix) in enumerate(findings_data, 1):
        if rc == 'HEALTHY':
            continue
        fix_num += 1
        pfc = pri_color(pri)
        rfc = rc_color(rc)
        hl  = (pri == 'P1')

        # Estimate row height from fix text length
        est_chars_per_line = 42
        words = fix.split()
        lines, line = [], ''
        for w_ in words:
            test = (line + ' ' + w_).strip()
            if len(test) <= est_chars_per_line:
                line = test
            else:
                if line:
                    lines.append(line)
                line = w_
        if line:
            lines.append(line)
        rh = max(22, len(lines) * 12 + 12)

        y = check_space(y, rh)
        bg = RED_BG if hl else (OFF_WHITE if fix_num % 2 == 0 else WHITE)
        box(c, LM, y - rh, CW, rh, bg, BORDER, 0.5)

        x = LM
        for (label, w, align), (val, color, font, size) in zip(fix_cols, [
            (str(fix_num), TEXT_MUTED, 'Helvetica-Bold', 9),
            (pri,     pfc,        'Helvetica-Bold', 9),
            (tn,      TEXT,       'Helvetica-Bold', 8.5),
            (rc,      rfc,        'Courier',        8),
            (fix,     TEXT_MID,   'Helvetica',      8.5),
        ]):
            if val == fix:
                wrap_text(c, x + 8, y - 12, val, w - 14,
                          font=font, size=size, color=color, lh=11)
            else:
                lx = x + (w / 2 if align == 'center' else 8)
                txt(c, lx, y - rh / 2 - 4,
                    val, font, size, color,
                    'center' if align == 'center' else 'left')
            vline(c, x + w, y - rh, y)
            x += w
        y -= rh

    # Governance notice
    y -= 14
    y = check_space(y, 70)
    governance_box(c, y)

    # ── SAVE ─────────────────────────────────────────────────
    # Note: footer already drawn by start_page() — do NOT call again
    c.save()
    buffer.seek(0)
    return buffer.read()


# ─── CONVENIENCE WRAPPER ──────────────────────────────────────────────────────
def generate_pdf_from_run(run_result: dict) -> bytes:
    """
    Adapter: maps _runs[run_id] dict to generate_report() input format.

    Actual run_result structure (from debug dump):
      run_id, status, started_at, demo_mode,
      recon: { tables: { <name>: { schema_diff, row_recon, cdc_analysis, issues, status } }, health_score, ... }
      agent_run: { health_score, critical_count, warning_count, root_causes_found, duration_seconds, total_tokens,
                   root_causes: [{ root_cause, confidence, affected_rows, recommended_fix, priority, table }] }
    """
    from datetime import datetime, timezone

    recon_data = run_result.get('recon', {})
    agent = run_result.get('agent_run', {}) or {}
    tables = recon_data.get('tables', {})
    rcs_list = agent.get('root_causes', [])

    # Derive counts from agent findings
    cr = sum(1 for r in rcs_list if r.get('priority') == 'P1') if rcs_list else recon_data.get('critical_count', 0)
    wr = sum(1 for r in rcs_list if r.get('priority') == 'P2') if rcs_list else recon_data.get('warning_count', 0)
    rc_count = sum(1 for r in rcs_list if r.get('root_cause') != 'HEALTHY') if rcs_list else cr + wr
    hp = agent.get('health_score', recon_data.get('health_score', 0))
    dur = agent.get('duration_seconds', 0)

    # Schema findings: iterate recon.tables dict
    schema_findings = []
    for tn, info in tables.items():
        sd = info.get('schema_diff', {})
        status = info.get('status', 'HEALTHY')
        issues = []
        for col in sd.get('missing_columns', []):
            issues.append(('MISSING', col.get('column', ''), col.get('type', ''), '\u2014', 'CRITICAL'))
        for m in sd.get('type_mismatches', []):
            issues.append(('TYPE_MISMATCH', m.get('column', ''), m.get('source_type', ''), m.get('target_type', ''), 'WARNING'))
        for col in sd.get('extra_columns', []):
            issues.append(('EXTRA', col.get('column', ''), '\u2014', col.get('type', ''), 'INFO'))
        # None = data-level only (has note but no schema issues)
        note = sd.get('note')
        if not issues and note:
            schema_findings.append((tn, status, None))
        else:
            schema_findings.append((tn, status, issues))

    # Row recon: iterate recon.tables dict
    recon_rows = []
    for tn, info in tables.items():
        r = info.get('row_recon', {})
        recon_rows.append((
            tn,
            r.get('source_count', 0),
            r.get('target_count', 0),
            r.get('delta', 0),
            f"{r.get('delta_pct', 0)}%",
            r.get('checksum_match', False),
            r.get('status', 'MATCH'),
        ))

    # CDC: iterate recon.tables dict + collect all gap_patterns
    cdc_rows = []
    all_patterns = []
    for tn, info in tables.items():
        cd = info.get('cdc_analysis', {})
        missed = cd.get('missed', 0)
        risk = 'HIGH' if missed > 100 else ('LOW' if missed > 0 else 'NONE')
        cdc_rows.append((
            tn,
            cd.get('total_events', 0),
            cd.get('captured', 0),
            missed,
            f"{cd.get('gap_rate', 0)}%",
            risk,
        ))
        for p in cd.get('gap_patterns', []):
            all_patterns.append((
                p.get('reason', ''),
                str(p.get('count', 0)),
                p.get('description', ''),
            ))

    # RCA findings: from agent_run.root_causes
    findings = []
    for rc in rcs_list:
        conf = rc.get('confidence', 0)
        findings.append((
            rc.get('table', ''),         # key is 'table' not 'table_name'
            rc.get('root_cause', ''),
            rc.get('priority', 'P3'),
            f"{conf * 100:.0f}%" if isinstance(conf, float) else str(conf),
            rc.get('affected_rows', 0),
            rc.get('recommended_fix', ''),
        ))

    report_data = {
        'run_id':       run_result.get('run_id', 'N/A'),
        'generated':    datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC'),
        'health_score': hp,
        'critical':     cr,
        'warnings':     wr,
        'tables_count': 5,
        'duration':     f"{dur}s",
        'root_causes':  rc_count,
        'schema':       schema_findings,
        'recon':        recon_rows,
        'cdc':          cdc_rows,
        'cdc_patterns': all_patterns,
        'findings':     findings,
    }

    return generate_report(report_data)
