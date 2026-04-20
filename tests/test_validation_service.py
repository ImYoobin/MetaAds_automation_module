from __future__ import annotations

import unittest

from dashboard.models import build_activity_id
from dashboard.services.url_service import build_cleaned_url_from_parts
from dashboard.services.validation_service import (
    build_history_execution_plan,
    validate_execution_modes,
)


def _sample_config() -> dict:
    return {
        "view_event_source": "CLICK_REPORTS_FROM_SIDE_NAV",
        "export_event_source": "CLICK_EXPORT_HISTORY_FROM_SIDE_NAV",
        "brands": [
            {
                "code": "brand_a",
                "name": "Brand A",
                "enabled": True,
                "activities": [
                    {
                        "name": "Activity 1",
                        "enabled": True,
                        "reports": {
                            "Overall": [
                                {
                                    "url": build_cleaned_url_from_parts(
                                        act_id="111",
                                        business_id="999",
                                        global_scope_id="999",
                                        report_id="rpt-1",
                                        event_source="CLICK_REPORTS_FROM_SIDE_NAV",
                                    )
                                }
                            ],
                            "Demo": [
                                {
                                    "url": build_cleaned_url_from_parts(
                                        act_id="111",
                                        business_id="999",
                                        global_scope_id="999",
                                        report_id="rpt-2",
                                        event_source="CLICK_REPORTS_FROM_SIDE_NAV",
                                    )
                                },
                                {
                                    "url": build_cleaned_url_from_parts(
                                        act_id="222",
                                        business_id="888",
                                        global_scope_id="888",
                                        report_id="rpt-3",
                                        event_source="CLICK_REPORTS_FROM_SIDE_NAV",
                                    )
                                },
                            ],
                            "Overall_BoF": [],
                            "Demo_BoF": [],
                            "Time": [],
                            "Time_BoF": [],
                        },
                    }
                ],
            }
        ],
    }


class ValidationServiceTests(unittest.TestCase):
    def test_validate_execution_modes_requires_at_least_one_option(self) -> None:
        reasons = validate_execution_modes(
            enable_report_download=False,
            enable_action_log_download=False,
        )

        self.assertEqual(reasons, ["최소 한 개의 실행 옵션을 선택해주세요."])

    def test_build_history_execution_plan_dedupes_account_targets(self) -> None:
        config = _sample_config()
        selected_ids = {
            build_activity_id(brand_code="brand_a", activity_name="Activity 1"),
        }

        plan = build_history_execution_plan(config, selected_ids)

        self.assertEqual(len(plan), 1)
        self.assertEqual(len(plan[0].account_targets), 2)
        self.assertEqual(
            [(target.act, target.business_id) for target in plan[0].account_targets],
            [("111", "999"), ("222", "888")],
        )


if __name__ == "__main__":
    unittest.main()
