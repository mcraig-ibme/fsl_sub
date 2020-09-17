#!/usr/bin/env python
import unittest
import yaml
import fsl_sub.coprocessors
import fsl_sub.config

from unittest.mock import patch

test_config = yaml.safe_load('''
thread_control:
  - OMP_NUM_THREADS
  - MKL_NUM_THREADS
  - MKL_DOMAIN_NUM_THREADS
  - OPENBLAS_NUM_THREADS
  - GOTO_NUM_THREADS
method: "grid"
method_opts:
    grid:
        queues: True
        large_job_split_pe: Null
        mail_support: False
        map_ram: False
        job_priorities: False
        array_holds: False
        array_limit: False
        architecture: False
        job_resources: False
        script_conf: False
        projects: False
queues:
    cuda.q:
        time: 18000
        max_size: 250
        slot_size: 64
        max_slots: 20
        copros:
            cuda:
                max_quantity: 4
                classes:
                    - K
                    - P
                    - V
        map_ram: true
        parallel_envs:
            - shmem
        priority: 1
        group: 0
        default: true
    phi.q:
        time: 1440
        max_size: 160
        slot_size: 4
        max_slots: 16
        copros:
            phi:
                max_quantity: 2
        map_ram: true
        parallel_envs:
            - shmem
        priority: 1
        group: 1
coproc_opts:
    cuda:
        resource: gpu
        classes: True
        class_resource: gputype
        class_types:
            G:
                resource: TitanX
                doc: TitanX. No-ECC, single-precision workloads
                capability: 1
            K:
                resource: k80
                doc: Kepler. ECC, double- or single-precision workloads
                capability: 2
            P:
                resource: v100
                doc: >
                    Pascal. ECC, double-, single- and half-precision
                    workloads
                capability: 3
            V:
                resource: v100
                doc: >
                    Volta. ECC, double-, single-, half-
                    and quarter-precision workloads
                capability: 4
        default_class: K
        include_more_capable: True
        uses_modules: True
        module_parent: cuda
    phi:
        resource: phi
        classes: False
        users_modules: True
        module_parent: phi
''')


# The following test doesn't work if run at the same time as the following
# tests - the patcher of the subsequent tests is getting in the way
# class TestNoCoprocessors(unittest.TestCase):
#     def setUp(self):
#         patcher = patch(
#             'fsl_sub.config.read_config', autospec=True)
#         self.addCleanup(patcher.stop)
#         self.mock_read_config = patcher.start()
#         self.mock_read_config.return_value = yaml.safe_load('''
# method: test
# method_opts:
#     test:
#         queues: True
# queues:
#     short.q:
#         time: 18000
#         max_size: 250
#         slot_size: 64
#         max_slots: 20
#         map_ram: true
#         parallel_envs:
#             - shmem
#         priority: 1
#         group: 0
#         default: true
# coproc_opts:
# ''')

#     def test_no_coprocs(self):
#         with self.subTest("coproc_info"):
#             self.assertDictEqual(
#                 fsl_sub.coprocessors.coproc_info(),
#                 {
#                     'available': None,
#                     'classes': None,
#                     'toolkits': None,
#                     }
#             )
#         with self.subTest('list_coprocessors'):
#             self.assertListEqual(
#                 fsl_sub.coprocessors.list_coprocessors(),
#                 []
#             )


class TestCoprocessors(unittest.TestCase):
    def setUp(self):
        fsl_sub.config.read_config.cache_clear()
        global test_config
        patcher = patch(
            'fsl_sub.config.read_config', autospec=True)
        self.addCleanup(patcher.stop)
        self.mock_read_config = patcher.start()
        self.mock_read_config.return_value = test_config

    def test_list_coprocessors(self):
        self.assertCountEqual(
            fsl_sub.coprocessors.list_coprocessors(),
            ['cuda', 'phi', ])

    def test_max_coprocessors(self):
        with self.subTest("Max CUDA"):
            self.assertEqual(
                fsl_sub.coprocessors.max_coprocessors(
                    'cuda'),
                4
            )
        with self.subTest("Max Phi"):
            self.assertEqual(
                fsl_sub.coprocessors.max_coprocessors(
                    'phi'),
                2
            )

    def test_coproc_classes(self):
        with self.subTest("CUDA classes"):
            self.assertListEqual(
                fsl_sub.coprocessors.coproc_classes(
                    'cuda'),
                ['K', 'P', 'V', ]
            )
        with self.subTest("Phi classes"):
            self.assertIsNone(
                fsl_sub.coprocessors.coproc_classes(
                    'phi'))

    @patch('fsl_sub.coprocessors.get_modules', autospec=True)
    def test_coproc_toolkits(self, mock_get_modules):
        with self.subTest("CUDA toolkits"):
            mock_get_modules.return_value = ['6.5', '7.0', '7.5', ]
            self.assertEqual(
                fsl_sub.coprocessors.coproc_toolkits(
                    'cuda'),
                ['6.5', '7.0', '7.5', ]
            )
            mock_get_modules.assert_called_once_with('cuda')

    @patch('fsl_sub.coprocessors.get_modules', autospec=True)
    @patch(
        'fsl_sub.coprocessors.coprocessor_config',
        autospec=True,
        return_value=test_config['coproc_opts']['cuda'])
    def test_coproc_get_module(self, mock_cpc, mock_get_modules):
        with self.subTest("CUDA toolkits"):
            mock_get_modules.return_value = ['6.5', '7.0', '7.5', ]
            self.assertEqual(
                fsl_sub.coprocessors.coproc_get_module('cuda', '7.5'),
                'cuda/7.5')
            mock_get_modules.assert_called_once_with('cuda')


if __name__ == '__main__':
    unittest.main()
