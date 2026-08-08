from __future__ import annotations

import html
import re
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    BaseDocTemplate,
    Flowable,
    Frame,
    KeepTogether,
    PageBreak,
    PageTemplate,
    Paragraph,
    Preformatted,
    Spacer,
    Table,
    TableStyle,
)


ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "docs" / "columnar-fields-and-indexes-guide-v3.md"
OUTPUT = ROOT / "output" / "pdf" / "worktable-columnar-side-indexes-guide-v3.pdf"

NAVY = colors.HexColor("#10243E")
INK = colors.HexColor("#17202A")
MUTED = colors.HexColor("#586678")
CYAN = colors.HexColor("#16B8C5")
PALE_CYAN = colors.HexColor("#EAF9FA")
PALE_BLUE = colors.HexColor("#F2F6FB")
LINE = colors.HexColor("#D9E2EC")
WHITE = colors.white


def clean_text(value: str) -> str:
    return (
        value.replace("\u2011", "-")
        .replace("\u2013", "-")
        .replace("\u2014", "-")
        .replace("\u2018", "'")
        .replace("\u2019", "'")
        .replace("\u201c", '"')
        .replace("\u201d", '"')
    )


def inline_markup(value: str) -> str:
    escaped = html.escape(clean_text(value))
    escaped = re.sub(
        r"`([^`]+)`",
        lambda match: f'<font name="Courier" color="#0B7180">{match.group(1)}</font>',
        escaped,
    )
    escaped = re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", escaped)
    return escaped


class ArchitectureFlow(Flowable):
    def __init__(self, width: float):
        super().__init__()
        self.width = width
        self.height = 41 * mm

    def draw(self):
        canvas = self.canv
        labels = [
            ("Primary key", "authoritative"),
            ("Tabular rows", "existing engine"),
            ("ColumnSlotId", "alignment"),
            ("Side vectors", "selected values"),
            ("Side index", "ordered access"),
        ]
        gap = 3 * mm
        box_width = (self.width - gap * (len(labels) - 1)) / len(labels)
        y = 10 * mm
        for index, (title, subtitle) in enumerate(labels):
            x = index * (box_width + gap)
            canvas.setFillColor(PALE_CYAN if index >= 2 else PALE_BLUE)
            canvas.setStrokeColor(CYAN if index >= 2 else LINE)
            canvas.roundRect(x, y, box_width, 19 * mm, 2 * mm, fill=1, stroke=1)
            canvas.setFillColor(NAVY)
            canvas.setFont("Helvetica-Bold", 8.2)
            canvas.drawCentredString(x + box_width / 2, y + 11.5 * mm, title)
            canvas.setFillColor(MUTED)
            canvas.setFont("Helvetica", 7.2)
            canvas.drawCentredString(x + box_width / 2, y + 6.5 * mm, subtitle)
            if index < len(labels) - 1:
                arrow_x = x + box_width + gap / 2
                canvas.setStrokeColor(CYAN)
                canvas.line(arrow_x - 1.2 * mm, y + 9.5 * mm, arrow_x + 1.2 * mm, y + 9.5 * mm)
                canvas.line(arrow_x + 1.2 * mm, y + 9.5 * mm, arrow_x, y + 10.7 * mm)
                canvas.line(arrow_x + 1.2 * mm, y + 9.5 * mm, arrow_x, y + 8.3 * mm)


class GuideDoc(BaseDocTemplate):
    def __init__(self, filename: str):
        super().__init__(
            filename,
            pagesize=A4,
            leftMargin=18 * mm,
            rightMargin=18 * mm,
            topMargin=20 * mm,
            bottomMargin=18 * mm,
            title="WorkTable Tabular + Columnar Side Indexes",
            author="PathScale",
            subject="Technical guide for WorkTable's additive columnar side-index proposal",
        )
        frame = Frame(
            self.leftMargin,
            self.bottomMargin,
            self.width,
            self.height,
            id="body",
            leftPadding=0,
            rightPadding=0,
            topPadding=0,
            bottomPadding=0,
        )
        self.addPageTemplates(PageTemplate(id="guide", frames=[frame], onPage=self.decorate_page))

    def decorate_page(self, canvas, doc):
        canvas.saveState()
        if canvas.getPageNumber() > 1:
            canvas.setStrokeColor(LINE)
            canvas.line(18 * mm, A4[1] - 13 * mm, A4[0] - 18 * mm, A4[1] - 13 * mm)
            canvas.setFillColor(MUTED)
            canvas.setFont("Helvetica", 7.5)
            canvas.drawString(18 * mm, A4[1] - 10 * mm, "WORKTABLE COLUMNAR SIDE INDEXES")
            canvas.drawRightString(A4[0] - 18 * mm, A4[1] - 10 * mm, "EXPERT REVIEW GUIDE")
        canvas.setFillColor(MUTED)
        canvas.setFont("Helvetica", 7.5)
        canvas.drawString(18 * mm, 9 * mm, "Tabular + columnar side indexes - v3")
        canvas.drawRightString(A4[0] - 18 * mm, 9 * mm, f"{canvas.getPageNumber()}")
        canvas.restoreState()


styles = getSampleStyleSheet()
styles.add(
    ParagraphStyle(
        "CoverEyebrow",
        fontName="Helvetica-Bold",
        fontSize=9,
        leading=11,
        textColor=CYAN,
        spaceAfter=7 * mm,
        alignment=TA_LEFT,
    )
)
styles.add(
    ParagraphStyle(
        "CoverTitle",
        fontName="Helvetica-Bold",
        fontSize=29,
        leading=32,
        textColor=NAVY,
        spaceAfter=5 * mm,
    )
)
styles.add(
    ParagraphStyle(
        "CoverDeck",
        fontName="Helvetica",
        fontSize=13,
        leading=18,
        textColor=MUTED,
        spaceAfter=10 * mm,
    )
)
styles.add(
    ParagraphStyle(
        "Section",
        fontName="Helvetica-Bold",
        fontSize=18,
        leading=22,
        textColor=NAVY,
        spaceBefore=6 * mm,
        spaceAfter=3.5 * mm,
        keepWithNext=True,
    )
)
styles.add(
    ParagraphStyle(
        "Subsection",
        fontName="Helvetica-Bold",
        fontSize=12.5,
        leading=16,
        textColor=NAVY,
        spaceBefore=4 * mm,
        spaceAfter=2 * mm,
        keepWithNext=True,
    )
)
styles.add(
    ParagraphStyle(
        "BodyTech",
        fontName="Helvetica",
        fontSize=9.4,
        leading=13.5,
        textColor=INK,
        spaceAfter=2.8 * mm,
    )
)
styles.add(
    ParagraphStyle(
        "BulletTech",
        parent=styles["BodyTech"],
        leftIndent=5 * mm,
        firstLineIndent=-3.5 * mm,
        bulletIndent=0,
        spaceAfter=1.4 * mm,
    )
)
styles.add(
    ParagraphStyle(
        "Callout",
        fontName="Helvetica-Bold",
        fontSize=10,
        leading=14.5,
        textColor=NAVY,
        backColor=PALE_CYAN,
        borderColor=CYAN,
        borderWidth=0.8,
        borderPadding=8,
        leftIndent=2 * mm,
        rightIndent=2 * mm,
        spaceBefore=2 * mm,
        spaceAfter=4 * mm,
    )
)
styles.add(
    ParagraphStyle(
        "CodeTech",
        fontName="Courier",
        fontSize=6.7,
        leading=9.2,
        textColor=colors.HexColor("#E8F2FA"),
        splitLongWords=False,
    )
)
styles.add(
    ParagraphStyle(
        "TableHeader",
        fontName="Helvetica-Bold",
        fontSize=8.3,
        leading=11.5,
        textColor=WHITE,
    )
)
styles.add(
    ParagraphStyle(
        "SmallMuted",
        fontName="Helvetica",
        fontSize=8.3,
        leading=11.5,
        textColor=MUTED,
        spaceAfter=2 * mm,
    )
)


def cover_story() -> list:
    pill = Table(
        [["SIDECAR ANALYTICS", "PRIMARY KEY PRESERVED", "FORMAT COMPATIBLE"]],
        colWidths=[54 * mm, 57 * mm, 54 * mm],
    )
    pill.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), PALE_CYAN),
                ("TEXTCOLOR", (0, 0), (-1, -1), NAVY),
                ("FONTNAME", (0, 0), (-1, -1), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 7),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("BOX", (0, 0), (-1, -1), 0.5, CYAN),
                ("INNERGRID", (0, 0), (-1, -1), 0.5, WHITE),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ]
        )
    )
    principles = Table(
        [
            [Paragraph("<b>Additive</b><br/><font color='#586678'>Opt in field by field.</font>", styles["BodyTech"]),
             Paragraph("<b>Compact</b><br/><font color='#586678'>Choose 8, 16, 32, or 64-bit slot positions.</font>", styles["BodyTech"]),
             Paragraph("<b>Safe</b><br/><font color='#586678'>Every result retains the authoritative primary key.</font>", styles["BodyTech"])],
        ],
        colWidths=[55 * mm, 55 * mm, 55 * mm],
    )
    principles.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), PALE_BLUE),
                ("BOX", (0, 0), (-1, -1), 0.5, LINE),
                ("INNERGRID", (0, 0), (-1, -1), 0.5, LINE),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ]
        )
    )
    return [
        Spacer(1, 11 * mm),
        Paragraph("WORKTABLE TECHNICAL GUIDE / AUGUST 2026", styles["CoverEyebrow"]),
        Paragraph("Tabular + columnar<br/>side indexes", styles["CoverTitle"]),
        Paragraph(
            "A complete expert-review guide to WorkTable's middle storage flavor: "
            "authoritative tabular rows plus derived columnar side structures.",
            styles["CoverDeck"],
        ),
        pill,
        Spacer(1, 10 * mm),
        ArchitectureFlow(165 * mm),
        Spacer(1, 4 * mm),
        principles,
        Spacer(1, 9 * mm),
        Paragraph(
            "Review focus: the three-flavor boundary, primary-key authority, bounded-slot behavior, "
            "mutation correctness, derived persistence, and honest HANA comparison.",
            styles["SmallMuted"],
        ),
        PageBreak(),
    ]


def parse_markdown(text: str) -> list:
    lines = clean_text(text).splitlines()
    story: list = []
    paragraph: list[str] = []
    bullets: list[str] = []
    code: list[str] = []
    table_rows: list[list[str]] = []
    quote_lines: list[str] = []
    in_code = False

    def flush_paragraph():
        nonlocal paragraph
        if paragraph:
            story.append(Paragraph(inline_markup(" ".join(part.strip() for part in paragraph)), styles["BodyTech"]))
            paragraph = []

    def flush_bullets():
        nonlocal bullets
        if bullets:
            for item in bullets:
                story.append(Paragraph(inline_markup(item), styles["BulletTech"], bulletText="-"))
            story.append(Spacer(1, 1.5 * mm))
            bullets = []

    def flush_code():
        nonlocal code
        if code:
            rendered = "\n".join(code).rstrip()
            code_block = Preformatted(rendered, styles["CodeTech"], maxLineLength=105)
            container = Table([[code_block]], colWidths=[165 * mm], hAlign="LEFT")
            container.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, -1), NAVY),
                        ("BOX", (0, 0), (-1, -1), 0.6, NAVY),
                        ("LEFTPADDING", (0, 0), (-1, -1), 8),
                        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                        ("TOPPADDING", (0, 0), (-1, -1), 7),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
                    ]
                )
            )
            story.extend([container, Spacer(1, 4 * mm)])
            code = []

    def flush_quote():
        nonlocal quote_lines
        if quote_lines:
            story.append(Paragraph(inline_markup(" ".join(quote_lines)), styles["Callout"]))
            quote_lines = []

    def flush_table():
        nonlocal table_rows
        if table_rows:
            rows = [row for row in table_rows if not all(re.fullmatch(r"\s*:?-+:?\s*", cell) for cell in row)]
            cells = []
            for row_index, row in enumerate(rows):
                style = styles["TableHeader"] if row_index == 0 else styles["SmallMuted"]
                cells.append([Paragraph(inline_markup(cell.strip()), style) for cell in row])
            widths = [40 * mm, 46 * mm, 79 * mm] if len(cells[0]) == 3 else None
            table = Table(cells, colWidths=widths, repeatRows=1, hAlign="LEFT")
            table.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
                        ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
                        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                        ("BACKGROUND", (0, 1), (-1, -1), PALE_BLUE),
                        ("BOX", (0, 0), (-1, -1), 0.5, LINE),
                        ("INNERGRID", (0, 0), (-1, -1), 0.5, LINE),
                        ("VALIGN", (0, 0), (-1, -1), "TOP"),
                        ("LEFTPADDING", (0, 0), (-1, -1), 6),
                        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                        ("TOPPADDING", (0, 0), (-1, -1), 5),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                    ]
                )
            )
            story.extend([table, Spacer(1, 4 * mm)])
            table_rows = []

    for line in lines:
        if not line.startswith("> ") and not in_code:
            flush_quote()
        if line.startswith("```"):
            flush_paragraph()
            flush_bullets()
            flush_table()
            if in_code:
                flush_code()
                in_code = False
            else:
                in_code = True
            continue
        if in_code:
            code.append(line)
            continue
        if line.startswith("# ") or line.startswith("## Add analytical access"):
            continue
        if line.startswith("## "):
            flush_paragraph()
            flush_bullets()
            flush_table()
            story.append(Paragraph(inline_markup(line[3:]), styles["Section"]))
            continue
        if line.startswith("### "):
            flush_paragraph()
            flush_bullets()
            flush_table()
            story.append(Paragraph(inline_markup(line[4:]), styles["Subsection"]))
            continue
        if line.startswith("> "):
            flush_paragraph()
            flush_bullets()
            flush_table()
            quote_lines.append(line[2:])
            continue
        if line.startswith("- "):
            flush_paragraph()
            flush_table()
            bullets.append(line[2:])
            continue
        if line.startswith("|") and line.endswith("|"):
            flush_paragraph()
            flush_bullets()
            table_rows.append([part.strip() for part in line.strip("|").split("|")])
            continue
        if not line.strip():
            flush_paragraph()
            flush_bullets()
            flush_table()
            continue
        if bullets:
            bullets[-1] = f"{bullets[-1]} {line.strip()}"
            continue
        paragraph.append(line)

    flush_paragraph()
    flush_bullets()
    flush_code()
    flush_table()
    flush_quote()
    return story


def main():
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    document = GuideDoc(str(OUTPUT))
    story = cover_story()
    story.extend(parse_markdown(SOURCE.read_text(encoding="utf-8")))
    document.build(story)
    print(OUTPUT)


if __name__ == "__main__":
    main()
