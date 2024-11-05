"""Tests import errors"""

import sys
import unittest
from unittest.mock import MagicMock, patch


class TestImport(unittest.TestCase):
    """Tests behavior of missing imports"""

    def test_module_not_found(self):
        """Test Module Not Found message"""

        with patch.dict(sys.modules):
            with self.assertRaises(ModuleNotFoundError) as e:
                sys.modules["aind_alert_utils"] = MagicMock()
                from aind_codeocean_pipeline_monitor.job import (  # noqa: F401
                    PipelineMonitorJob,
                )

        expected_message = (
            "Running jobs requires all dependencies: \n"
            "  'pip install aind-codeocean-pipeline-monitor[full]'\n"
            "See README for more information."
        )
        self.assertEqual(expected_message, e.exception.msg)


if __name__ == "__main__":
    unittest.main()
