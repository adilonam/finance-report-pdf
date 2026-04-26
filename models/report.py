from dataclasses import asdict, dataclass
from datetime import date


@dataclass
class ReportData:
    report_title: str
    report_date: str
    company_name: str
    author_name: str
    summary_text: str
    revenue: str
    expenses: str
    net_profit: str

    @classmethod
    def default(cls) -> "ReportData":
        return cls(
            report_title="Monthly Finance Report",
            report_date=str(date.today()),
            company_name="Demo Finance Ltd.",
            author_name="Analyst Team",
            summary_text=(
                "Revenue grew by 12% month-over-month while operating expenses "
                "increased by 4%, resulting in stronger net margins."
            ),
            revenue="EUR 125,000",
            expenses="EUR 73,000",
            net_profit="EUR 52,000",
        )

    def to_dict(self) -> dict:
        return asdict(self)
