#!/usr/bin/env python
import unittest
import yaml
import coprocessors

from unittest.mock import patch


class TestCoprocessors(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = yaml.load('''
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
copro_opts:
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

    def test_list_coprocessors(self):
        self.assertCountEqual(
            coprocessors.list_coprocessors(self.config),
            ['cuda', 'phi', ])

    def test_max_coprocessors(self):
        with self.subTest("Max CUDA"):
            self.assertEqual(
                coprocessors.max_coprocessors(
                    self.config,
                    'cuda'),
                4
            )
        with self.subTest("Max Phi"):
            self.assertEqual(
                coprocessors.max_coprocessors(
                    self.config,
                    'phi'),
                2
            )

    def test_coproc_classes(self):
        with self.subTest("CUDA classes"):
            self.assertListEqual(
                coprocessors.coproc_classes(
                    self.config,
                    'cuda'),
                ['K', 'P', 'V', ]
            )
        with self.subTest("Phi classes"):
            self.assertIsNone(
                coprocessors.coproc_classes(
                    self.config,
                    'phi'))

    @patch('coprocessors.get_modules', auto_spec=True)
    def test_coproc_toolkits(self, mock_get_modules):
        with self.subTest("CUDA toolkits"):
            mock_get_modules.return_value = ['6.5', '7.0', '7.5', ]
            self.assertEqual(
                coprocessors.coproc_toolkits(
                        self.config,
                        'cuda'),
                ['6.5', '7.0', '7.5', ]
                )
            mock_get_modules.assert_called_once_with('cuda')
#  This isn't really a useful test!


if __name__ == '__main__':
    unittest.main()