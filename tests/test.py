#!/usr/bin/env python
import getpass
import socket
import unittest
import yaml
import fsl_sub


YAML_CONF = '''---
method: SGE
ram_units: G
method_opts:
    SGE:
        parallel_envs:
        - shmem
        - specialpe
        same_node_pes:
        - shmem
        - specialpe
        large_job_split_pe: shmem
        copy_environment: True
        affinity_type: linear
        affinity_control: threads
        mail_support: True
        mail_modes:
            b:
                - b
            e:
                - e
            a:
                - a
            f:
                - a
                - e
                - b
            n:
                - n
        mail_mode: a
        map_ram: True
        ram_resources:
            - m_mem_free
            - h_vmem
        job_priorities: True
        min_priority: -1023
        max_priority: 0
        array_holds: True
        array_limits: True
        architecture: False
        job_resources: True
coproc_opts:
  cuda:
    resource: gpu
    classes: True
    class_resource: gputype
    class_types:
      K:
        resource: k80
        doc: Kepler. ECC, double- or single-precision workloads
        capability: 2
      P:
        resource: p100
        doc: >
          Pascal. ECC, double-, single- and half-precision
          workloads
        capability: 3
    default_class: K
    include_more_capable: True
    uses_modules: True
    module_parent: cuda
queues:
  gpu.q:
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
  a.qa,a.qb,a.qc:
    time: 1440
    max_size: 160
    slot_size: 4
    max_slots: 16
    map_ram: true
    parallel_envs:
      - shmem
    priority: 3
    group: 1
    default: true
  a.qa,a.qc:
    time: 1440
    max_size: 240
    slot_size: 16
    max_slots: 16
    map_ram: true
    parallel_envs:
      - shmem
    priority: 2
    group: 1
    default: true
  a.qc:
    time: 1440
    max_size: 368
    slot_size: 16
    max_slots: 24
    map_ram: true
    parallel_envs:
      - shmem
    priority: 1
    group: 1
    default: true
  b.qa,b.qb,b.qc:
    time: 10080
    max_size: 160
    slot_size: 4
    max_slots: 16
    map_ram: true
    parallel_envs:
      - shmem
    priority: 3
    group: 2
  b.qa,b.qc:
    time: 10080
    max_size: 240
    slot_size: 16
    max_slots: 16
    map_ram: true
    parallel_envs:
      - shmem
    priority: 2
    group: 2
  b.qc:
    time: 10080
    max_size: 368
    slot_size: 16
    max_slots: 24
    map_ram: true
    parallel_envs:
      - shmem
    priority: 1
    group: 2
  t.q:
    time: 10080
    max_size: 368
    slot_size: 16
    max_slots: 24
    map_ram: true
    parallel_envs:
      - specialpe
    priority: 1
    group: 2

default_queues:
  - a.qa,a,qb,a.qc
  - a.qa,a.qc
  - a.qc

'''
USER_EMAIL = "{username}@{hostname}".format(
                    username=getpass.getuser(),
                    hostname=socket.gethostname()
                )


class SubmitTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conf_dict = yaml.load(YAML_CONF)
# This needs some tests writing:

# submit with command = []
# submit with command = 'a string of commands'
# submit with command = ['/usr/bin/command']


class GetQTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conf_dict = yaml.load(YAML_CONF)

    def test_getq_and_slots(self):
        with self.subTest('All a queues'):
            self.assertTupleEqual(
                ('a.qa,a.qb,a.qc', 1, ),
                fsl_sub.getq_and_slots(
                    self.conf_dict['queues'],
                    job_time=1000)
            )
        with self.subTest('Default queue'):
            self.assertTupleEqual(
                ('a.qa,a.qb,a.qc', 1, ),
                fsl_sub.getq_and_slots(
                    self.conf_dict['queues'])
            )
        with self.subTest("More RAM"):
            self.assertTupleEqual(
                ('a.qa,a.qc', 13, ),
                fsl_sub.getq_and_slots(
                    self.conf_dict['queues'],
                    job_time=1000,
                    job_ram=200)
            )
        with self.subTest("No time"):
            self.assertTupleEqual(
                ('a.qa,a.qc', 13, ),
                fsl_sub.getq_and_slots(
                    self.conf_dict['queues'],
                    job_ram=200)
            )
        with self.subTest("More RAM"):
            self.assertTupleEqual(
                ('a.qc', 19, ),
                fsl_sub.getq_and_slots(
                    self.conf_dict['queues'],
                    job_ram=300)
            )
        with self.subTest('Longer job'):
            self.assertTupleEqual(
                ('b.qa,b.qb,b.qc', 1, ),
                fsl_sub.getq_and_slots(
                    self.conf_dict['queues'],
                    job_time=2000)
            )
        with self.subTest('Too long job'):
            self.assertRaises(
                fsl_sub.BadSubmission,
                fsl_sub.getq_and_slots(
                    self.conf_dict['queues'],
                    job_time=200000)
            )
        with self.subTest("2x RAM"):
            self.assertIsNone(
                fsl_sub.getq_and_slots(
                    self.conf_dict['queues'],
                    job_ram=600)
            )
        with self.subTest('PE'):
            self.assertTupleEqual(
                ('t.q', 1, ),
                fsl_sub.getq_and_slots(
                    self.conf_dict['queues'],
                    ll_env="specialpe")
            )
        with self.subTest('PE missing'):
            self.assertIsNone(
                fsl_sub.getq_and_slots(
                    self.conf_dict['queues'],
                    ll_env="unknownpe")
            )
        with self.subTest('GPU'):
            self.assertTupleEqual(
                ('gpu.q', 1, ),
                fsl_sub.getq_and_slots(
                    self.conf_dict['queues'],
                    coprocessor='cuda')
            )
        with self.subTest("job ram is none"):
            self.assertTupleEqual(
                ('a.qa,a.qb,a.qc', 1, ),
                fsl_sub.getq_and_slots(
                    self.conf_dict['queues'],
                    job_ram=None)
            )
        with self.subTest("job time is none"):
            self.assertTupleEqual(
                ('a.qa,a.qb,a.qc', 1, ),
                fsl_sub.getq_and_slots(
                    self.conf_dict['queues'],
                    job_time=None)
            )


if __name__ == '__main__':
    unittest.main()
