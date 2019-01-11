#!/usr/bin/env python
import unittest
import fsl_sub.projects

from unittest.mock import patch


class TestConfig(unittest.TestCase):
    @patch('fsl_sub.projects.project_list', autospec=True)
    def test_project_exists(self, mock_project_list):
        mock_project_list.return_value = ['aproject', 'bproject', ]

        self.assertTrue(fsl_sub.projects.project_exists('aproject'))
        self.assertFalse(fsl_sub.projects.project_exists('cproject'))

    @patch('fsl_sub.projects.project_list', autospec=True)
    def test_projects(self, mock_project_list):
        mock_project_list.return_value = ['aproject', 'bproject', ]

        with self.subTest("Test get from environment 1"):
            with patch.dict(
                    'fsl_sub.projects.os.environ',
                    {'FSLSUB_PROJECT': 'AB'}, clear=True):
                self.assertEqual('AB', fsl_sub.projects.get_project_env(None))
        with self.subTest("Test get from environment 2"):
            with patch.dict(
                    'fsl_sub.projects.os.environ',
                    {'FSLSUB_PROJECT': 'AB'}, clear=True):
                self.assertEqual('CD', fsl_sub.projects.get_project_env('CD'))
        with self.subTest("Test environment is empty"):
            with patch.dict('fsl_sub.projects.os.environ', {}, clear=True):
                self.assertIsNone(fsl_sub.projects.get_project_env(None))
