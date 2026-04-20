from __future__ import annotations

import shutil
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from meta_core import pathing as pathing_module
from meta_core import runtime as runtime_module


class MetaUserDataPathTests(unittest.TestCase):
    def _temp_root(self) -> Path:
        temp_parent = (Path.cwd() / ".tmp_user_data_path_tests").resolve()
        temp_parent.mkdir(parents=True, exist_ok=True)
        root = (temp_parent / f"user_data_path_test_{uuid.uuid4().hex}").resolve()
        root.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(root, ignore_errors=True))
        return root

    def test_build_meta_shared_user_data_dir_is_browser_scoped(self) -> None:
        root = self._temp_root()

        edge_dir = pathing_module.build_meta_shared_user_data_dir(root, "msedge")
        chrome_dir = pathing_module.build_meta_shared_user_data_dir(root, "chrome")

        self.assertEqual(edge_dir, root / "MetaAdsExport" / "user_data" / "meta" / "msedge")
        self.assertEqual(chrome_dir, root / "MetaAdsExport" / "user_data" / "meta" / "chrome")

    def test_prepare_meta_user_data_dir_moves_legacy_profile_into_shared_profile(self) -> None:
        root = self._temp_root()
        requested_dir = pathing_module.build_meta_shared_user_data_dir(root, "msedge")
        legacy_dir = pathing_module.build_legacy_meta_history_user_data_dir(root)
        legacy_dir.mkdir(parents=True, exist_ok=True)
        (legacy_dir / "Cookies").write_text("cookie", encoding="utf-8")

        prepared = pathing_module.prepare_meta_user_data_dir(requested_dir=requested_dir)

        self.assertEqual(prepared.migration_mode, "move")
        self.assertEqual(prepared.effective_dir, requested_dir)
        self.assertTrue((requested_dir / "Cookies").exists())
        self.assertFalse(legacy_dir.exists())

    def test_prepare_meta_user_data_dir_copies_legacy_profile_when_move_fails(self) -> None:
        root = self._temp_root()
        requested_dir = pathing_module.build_meta_shared_user_data_dir(root, "msedge")
        legacy_dir = pathing_module.build_legacy_meta_history_user_data_dir(root)
        legacy_dir.mkdir(parents=True, exist_ok=True)
        (legacy_dir / "Cookies").write_text("cookie", encoding="utf-8")

        with patch.object(pathing_module.shutil, "move", side_effect=OSError("locked")):
            prepared = pathing_module.prepare_meta_user_data_dir(requested_dir=requested_dir)

        self.assertEqual(prepared.migration_mode, "copy")
        self.assertEqual(prepared.effective_dir, requested_dir)
        self.assertTrue((requested_dir / "Cookies").exists())
        self.assertTrue(legacy_dir.exists())
        self.assertIn("move_failed=", prepared.warning)

    def test_prepare_meta_user_data_dir_falls_back_to_legacy_when_move_and_copy_fail(self) -> None:
        root = self._temp_root()
        requested_dir = pathing_module.build_meta_shared_user_data_dir(root, "msedge")
        legacy_dir = pathing_module.build_legacy_meta_history_user_data_dir(root)
        legacy_dir.mkdir(parents=True, exist_ok=True)
        (legacy_dir / "Cookies").write_text("cookie", encoding="utf-8")

        with (
            patch.object(pathing_module.shutil, "move", side_effect=OSError("locked")),
            patch.object(pathing_module.shutil, "copytree", side_effect=OSError("copy failed")),
        ):
            prepared = pathing_module.prepare_meta_user_data_dir(requested_dir=requested_dir)

        self.assertEqual(prepared.migration_mode, "legacy_fallback")
        self.assertEqual(prepared.effective_dir, legacy_dir)
        self.assertIn("legacy_profile_migration_failed", prepared.warning)

    def test_build_sb_kwargs_keeps_requested_user_data_dir(self) -> None:
        class _DummyMeta:
            def _build_sb_kwargs(self, browser: str) -> dict[str, object]:
                return {
                    "browser": browser,
                    "user_data_dir": "legacy-profile",
                    "uc": True,
                    "uc_subprocess": True,
                }

        root = self._temp_root()
        requested_dir = (root / "shared-profile").resolve()

        kwargs = runtime_module.build_sb_kwargs(
            _DummyMeta(),
            "edge",
            user_data_dir=requested_dir,
        )

        self.assertEqual(kwargs["user_data_dir"], str(requested_dir))
        self.assertFalse(kwargs["incognito"])
        self.assertFalse(kwargs["guest_mode"])


if __name__ == "__main__":
    unittest.main()
