#!/usr/bin/python
import argparse
import copy
import getpass
import io
import os
from ruamel.yaml import (YAML, YAMLError, )
import socket
import sys
import unittest
import fsl_sub.cmdline
from unittest.mock import patch

YAML_CONF = '''---
method: sge
modulecmd: /usr/bin/modulecmd
thread_control:
  - OMP_NUM_THREADS
  - MKL_NUM_THREADS
  - MKL_DOMAIN_NUM_THREADS
  - OPENBLAS_NUM_THREADS
  - GOTO_NUM_THREADS
preserve_modules: True
export_vars: []
method_opts:
    sge:
        queues: True
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
        array_limit: True
        architecture: False
        job_resources: True
        projects: False
        script_conf: True
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
    no_binding: True
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
YAML_CONF_PROJECTS = '''---
method: sge
modulecmd: /usr/bin/modulecmd
thread_control:
  - OMP_NUM_THREADS
  - MKL_NUM_THREADS
  - MKL_DOMAIN_NUM_THREADS
  - OPENBLAS_NUM_THREADS
  - GOTO_NUM_THREADS
preserve_modules: True
export_vars: []
method_opts:
    sge:
        queues: True
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
        array_limit: True
        architecture: False
        job_resources: True
        projects: True
        script_conf: True
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
    no_binding: True
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


class FakePlugin(object):
    def submit(self):
        pass

    def qtest(self):
        pass

    def queue_exists(self):
        pass

    def plugin_version(self):
        return '1.2.0'

    def already_queued(self):
        return False


class TestMisc(unittest.TestCase):
    def test_titlize_key(self):
        self.assertEqual(
            'A Word',
            fsl_sub.utils.titlize_key(
                'a_word'
            )
        )

    def test_blank_none(self):
        self.assertEqual(
            fsl_sub.utils.blank_none(1),
            '1'
        )
        self.assertEqual(
            fsl_sub.utils.blank_none(None),
            ''
        )
        self.assertEqual(
            fsl_sub.utils.blank_none('A'),
            'A'
        )
        self.assertEqual(
            fsl_sub.utils.blank_none(['a', 'b']),
            "['a', 'b']"
        )


@patch(
    'fsl_sub.cmdline.load_plugins',
    autospec=True,
    return_value={'fsl_sub_plugin_sge': FakePlugin()}
)
@patch(
    'fsl_sub.shell_modules.read_config',
    autospec=True,
    return_value=YAML(typ='safe').load(YAML_CONF))
@patch(
    'fsl_sub.cmdline.read_config',
    autospec=True,
    return_value=YAML(typ='safe').load(YAML_CONF))
@patch(
    'fsl_sub.config.read_config',
    autospec=True,
    return_value=YAML(typ='safe').load(YAML_CONF))
@patch(
    'fsl_sub.cmdline.submit',
    autospec=True,
    return_value=123)
@patch(
    'fsl_sub.cmdline.get_modules', autospec=True,
    return_value=['7.5', '8.0', ])
@patch(
    'fsl_sub.coprocessors.get_modules',
    autospec=True, return_value=['7.5', '8.0', ])
class TestMain(unittest.TestCase):
    def setUp(self):
        self.yaml = YAML(typ='safe')
        self. base_args = {
            'architecture': None,
            'array_hold': None,
            'array_limit': None,
            'array_specifier': None,
            'array_task': False,
            'coprocessor': None,
            'coprocessor_toolkit': None,
            'export_vars': [],
            'coprocessor_class': None,
            'coprocessor_class_strict': False,
            'coprocessor_multi': 1,
            'name': None,
            'parallel_env': None,
            'queue': None,
            'threads': 1,
            'jobhold': None,
            'jobram': None,
            'jobtime': None,
            'keep_jobscript': False,
            'logdir': None,
            'mail_on': None,
            'mailto': USER_EMAIL,
            'priority': None,
            'ramsplit': True,
            'requeueable': True,
            'resources': None,
            'usescript': False,
            'validate_command': True,
            'as_tuple': False,
            'project': None
        }

    def test_noramsplit(self, *args):
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmdline.main(['--noramsplit', '1', '2', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )
        test_args = copy.deepcopy(self.base_args)
        test_args['ramsplit'] = False
        args[2].assert_called_with(
            ['1', '2', ],
            **test_args
        )

    def test_parallelenv(self, *args):
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmdline.main(['--parallelenv', 'shmem,2', '1', '2', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )
        test_args = copy.deepcopy(self.base_args)
        test_args['parallel_env'] = 'shmem'
        test_args['threads'] = 2
        args[2].assert_called_with(
            ['1', '2', ],
            **test_args
        )
        args[2].reset_mock()
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmdline.main(['-s', 'shmem,2', '1', '2', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )
        test_args = copy.deepcopy(self.base_args)
        test_args['parallel_env'] = 'shmem'
        test_args['threads'] = 2
        args[2].assert_called_with(
            ['1', '2', ],
            **test_args
        )

    def test_mailoptions(self, *args):
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmdline.main(['--mailoptions', 'n', '1', '2', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )
        test_args = copy.deepcopy(self.base_args)
        test_args['mail_on'] = 'n'
        args[2].assert_called_with(
            ['1', '2', ],
            **test_args
        )

    def test_mailto(self, *args):
        mailto = 'user@test.com'
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmdline.main(['--mailto', mailto, '1', '2', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )
        test_args = copy.deepcopy(self.base_args)
        test_args['mailto'] = mailto
        args[2].assert_called_with(
            ['1', '2', ],
            **test_args
        )
        args[2].reset_mock()
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmdline.main(['-M', mailto, '1', '2', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )

        args[2].assert_called_with(
            ['1', '2', ],
            **test_args
        )

    def test_array_task(self, *args):
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmdline.main(['--array_task', 'taskfile', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )
        test_args = copy.deepcopy(self.base_args)
        test_args['array_task'] = True
        args[2].assert_called_with(
            'taskfile',
            **test_args
        )
        args[2].reset_mock()
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmdline.main(['-t', 'taskfile', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )
        args[2].assert_called_with(
            'taskfile',
            **test_args
        )

    def test_array_limit(self, *args):
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmdline.main(
                ['--array_task', 'commandfile', '--array_limit', '2', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )
        test_args = copy.deepcopy(self.base_args)
        test_args['array_task'] = True
        test_args['array_limit'] = 2
        args[2].assert_called_with(
            'commandfile',
            **test_args
        )
        args[2].reset_mock()
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmdline.main(['-x', '2', '--array_task', 'commandfile', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )
        test_args = copy.deepcopy(self.base_args)
        test_args['array_task'] = True
        test_args['array_limit'] = 2
        args[2].assert_called_with(
            'commandfile',
            **test_args
        )

    def test_array_hold(self, *args):
        hold_id = '20002'
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmdline.main(
                ['--array_task', 'commandfile', '--array_hold', hold_id, ])
            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )
        test_args = copy.deepcopy(self.base_args)
        test_args['array_task'] = True
        test_args['array_hold'] = [hold_id, ]
        args[2].assert_called_with(
            'commandfile',
            **test_args
        )

    def test_job_hold(self, *args):
        hold_id = '20002'
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmdline.main(
                ['--jobhold', hold_id, 'commandfile'])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )
        test_args = copy.deepcopy(self.base_args)
        test_args['jobhold'] = [hold_id, ]
        args[2].assert_called_with(
            ['commandfile'],
            **test_args
        )

    def test_array_native(self, *args):
        array_desc = '1-4:2'
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmdline.main(
                ['--array_native', array_desc, 'command', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )
        test_args = copy.deepcopy(self.base_args)
        test_args['array_task'] = True
        test_args['array_specifier'] = array_desc
        args[2].assert_called_with(
            ['command'],
            **test_args
        )

    def test_coprocessor(self, *args):
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmdline.main(['--coprocessor', 'cuda', '1', '2', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )
        test_args = copy.deepcopy(self.base_args)
        test_args['coprocessor'] = 'cuda'

        args[2].assert_called_with(
            ['1', '2', ],
            **test_args
        )

    def test_coprocessor_toolkit(self, *args):
        with io.StringIO() as text_trap:
            sys.stdout = text_trap
            fsl_sub.cmdline.main([
                '--coprocessor', 'cuda',
                '--coprocessor_toolkit', '7.5',
                '1', '2', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )
        test_args = copy.deepcopy(self.base_args)
        test_args['coprocessor'] = 'cuda'
        test_args['coprocessor_toolkit'] = '7.5'
        args[2].assert_called_with(
            ['1', '2', ],
            **test_args
        )

    def test_coprocessor_class(self, *args):
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmdline.main([
                '--coprocessor', 'cuda',
                '--coprocessor_class', 'K',
                '1', '2', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )
        test_args = copy.deepcopy(self.base_args)
        test_args['coprocessor'] = 'cuda'
        test_args['coprocessor_class'] = 'K'
        args[2].assert_called_with(
            ['1', '2', ],
            **test_args
        )

    def test_coprocessor_class_strict(self, *args):
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmdline.main([
                '--coprocessor', 'cuda',
                '--coprocessor_class', 'K',
                '--coprocessor_class_strict',
                '1', '2', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )
        test_args = copy.deepcopy(self.base_args)
        test_args['coprocessor'] = 'cuda'
        test_args['coprocessor_class'] = 'K'
        test_args['coprocessor_class_strict'] = True
        args[2].assert_called_with(
            ['1', '2', ],
            **test_args
        )

    def test_coprocessor_multi(self, *args):
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmdline.main([
                '--coprocessor', 'cuda',
                '--coprocessor_multi', '2',
                '1', '2', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )
        test_args = copy.deepcopy(self.base_args)
        test_args['coprocessor'] = 'cuda'
        test_args['coprocessor_multi'] = '2'
        args[2].assert_called_with(
            ['1', '2', ],
            **test_args
        )

    def test_project(self, *args):
        args[3].return_value = self.yaml.load(YAML_CONF_PROJECTS)
        args[4].return_value = self.yaml.load(YAML_CONF_PROJECTS)
        args[5].return_value = self.yaml.load(YAML_CONF_PROJECTS)
        with patch(
                'fsl_sub.projects.read_config',
                autospec=True,
                return_value=self.yaml.load(YAML_CONF_PROJECTS)):
            with io.StringIO() as text_trap:
                sys.stdout = text_trap

                fsl_sub.cmdline.main(['--project', 'Aproject', '1', '2', ])

                sys.stdout = sys.__stdout__

                self.assertEqual(
                    text_trap.getvalue(),
                    '123\n'
                )
            test_args = copy.deepcopy(self.base_args)
            test_args['project'] = 'Aproject'
            args[2].assert_called_with(
                ['1', '2', ],
                **test_args
            )

    def test_project_env(self, *args):
        args[3].return_value = self.yaml.load(YAML_CONF_PROJECTS)
        args[4].return_value = self.yaml.load(YAML_CONF_PROJECTS)
        args[5].return_value = self.yaml.load(YAML_CONF_PROJECTS)
        with patch(
                'fsl_sub.projects.read_config',
                autospec=True,
                return_value=self.yaml.load(YAML_CONF_PROJECTS)):
            with patch.dict(
                    'fsl_sub.projects.os.environ',
                    {'FSLSUB_PROJECT': 'Bproject', }, clear=True):
                with io.StringIO() as text_trap:
                    sys.stdout = text_trap

                    fsl_sub.cmdline.main(['1', '2', ])

                    sys.stdout = sys.__stdout__

                    self.assertEqual(
                        text_trap.getvalue(),
                        '123\n')
                test_args = copy.deepcopy(self.base_args)
                test_args['project'] = 'Bproject'
                args[2].assert_called_with(
                    ['1', '2', ],
                    **test_args
                )

    def test_comma_sep_export(self, *args):
        args[3].return_value = self.yaml.load(YAML_CONF_PROJECTS)
        args[4].return_value = self.yaml.load(YAML_CONF_PROJECTS)
        args[5].return_value = self.yaml.load(YAML_CONF_PROJECTS)
        with patch(
            'fsl_sub.projects.read_config',
                autospec=True,
                return_value=self.yaml.load(YAML_CONF_PROJECTS)):
            with io.StringIO() as text_trap:
                sys.stdout = text_trap

                fsl_sub.cmdline.main(['--export=avar=b,c,d', 'command', ])

                sys.stdout = sys.__stdout__

                self.assertEqual(
                    text_trap.getvalue(),
                    '123\n')
            test_args = copy.deepcopy(self.base_args)
            test_args['export_vars'] = ['avar=b,c,d']
            args[2].assert_called_with(
                ['command', ],
                **test_args
            )

    def test_comma_sep_exports(self, *args):
        args[3].return_value = self.yaml.load(YAML_CONF_PROJECTS)
        args[4].return_value = self.yaml.load(YAML_CONF_PROJECTS)
        args[5].return_value = self.yaml.load(YAML_CONF_PROJECTS)
        with patch(
            'fsl_sub.projects.read_config',
                autospec=True,
                return_value=self.yaml.load(YAML_CONF_PROJECTS)):
            with io.StringIO() as text_trap:
                sys.stdout = text_trap

                fsl_sub.cmdline.main(['--export=avar=b,c,d', '--export=bvar=c', 'command', ])

                sys.stdout = sys.__stdout__

                self.assertEqual(
                    text_trap.getvalue(),
                    '123\n')
            test_args = copy.deepcopy(self.base_args)
            test_args['export_vars'] = ['avar=b,c,d', 'bvar=c', ]
            args[2].assert_called_with(
                ['command', ],
                **test_args
            )


class ErrorRaisingArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise ValueError(message)  # reraise an error


class TestExampleConf(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = fsl_sub.cmdline.example_config_parser(
            parser_class=ErrorRaisingArgumentParser)
        none_config = os.path.join(
            os.path.dirname(__file__), '..',
            'plugins', 'fsl_sub_shell.yml')
        with open(none_config, 'r') as yfile:
            cls.exp_conf = yfile.read()

    def test_example_config_parser_blank(self):
        self.assertRaises(
            ValueError,
            self.parser.parse_args,
            ['', ]
        )

    def test_example_config_parser_unknown_plugin(self):
        self.assertRaises(
            ValueError,
            self.parser.parse_args,
            ['NoCluster', ]
        )

    def test_example_config_parser_known_plugin(self):
        self.assertEqual(
            self.parser.parse_args(['shell']).plugin,
            'shell'
        )

    def test_example_config(self):
        with unittest.mock.patch('fsl_sub.cmdline.sys.stdout', new_callable=io.StringIO) as mock_stdout:
            fsl_sub.cmdline.example_config(['shell', ])
        output = mock_stdout.getvalue()
        try:
            YAML().load(output)
        except YAMLError:
            self.fail("Example config not valid YAML")


@patch(
    'fsl_sub.cmdline.find_fsldir', autospec=True,
    return_value='/usr/local/fsl'
)
@patch(
    'fsl_sub.cmdline.conda_check_update', autospec=True
)
@patch(
    'fsl_sub.cmdline.available_plugin_packages', autospec=True,
    return_value=['fsl_sub_plugin_sge', ]
)
class TestUpdate(unittest.TestCase):
    def test_update_check(
            self, mock_pp,
            mock_cup, mock_fsldir):

        mock_cup.return_value = {
            'fsl_sub': {'version': '2.0.0', 'old_version': '1.0.0', },
        }
        # Trap stdout
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmdline.update(args=['-c'])
            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '''Available updates:
fsl_sub (1.0.0 -> 2.0.0)
'''
            )
        mock_cup.assert_called_with(
            fsldir='/usr/local/fsl',
            packages=['fsl_sub', 'fsl_sub_plugin_sge', ]
        )

    @patch(
        'fsl_sub.cmdline.conda_update', autospec=True)
    def test_update_noquestion(
            self, mock_up, mock_pp,
            mock_cup, mock_fsldir):

        mock_cup.return_value = {
            'fsl_sub': {'version': '2.0.0', 'old_version': '1.0.0', },
        }
        mock_up.return_value = {
            'fsl_sub': {'version': '2.0.0', 'old_version': '1.0.0', },
        }
        # Trap stdout
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmdline.update(args=['-y'])
            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '''Available updates:
fsl_sub (1.0.0 -> 2.0.0)
fsl_sub updated.
'''
            )
        mock_cup.assert_called_with(
            fsldir='/usr/local/fsl',
            packages=['fsl_sub', 'fsl_sub_plugin_sge', ]
        )

    @patch(
        'fsl_sub.cmdline.conda_update', autospec=True)
    @patch(
        'fsl_sub.cmdline.user_input', autospec=True
    )
    def test_update_ask(
            self, mock_input, mock_up, mock_pp,
            mock_cup, mock_fsldir):

        mock_cup.return_value = {
            'fsl_sub': {'version': '2.0.0', 'old_version': '1.0.0', },
        }
        mock_up.return_value = {
            'fsl_sub': {'version': '2.0.0', 'old_version': '1.0.0', },
        }
        mock_input.return_value = 'y'
        # Trap stdout
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmdline.update(args=[])
            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '''Available updates:
fsl_sub (1.0.0 -> 2.0.0)
fsl_sub updated.
'''
            )
        mock_input.assert_called_once_with('Install pending updates? ')
        mock_input.reset_mock()
        mock_cup.assert_called_with(
            fsldir='/usr/local/fsl',
            packages=['fsl_sub', 'fsl_sub_plugin_sge', ]
        )

        mock_cup.reset_mock()
        mock_input.return_value = 'yes'
        # Trap stdout
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmdline.update(args=[])
            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '''Available updates:
fsl_sub (1.0.0 -> 2.0.0)
fsl_sub updated.
'''
            )
        mock_cup.assert_called_with(
            fsldir='/usr/local/fsl',
            packages=['fsl_sub', 'fsl_sub_plugin_sge', ]
        )
        mock_input.assert_called_once_with('Install pending updates? ')
        mock_input.reset_mock()

        mock_cup.reset_mock()
        mock_input.return_value = 'no'

        # Trap stdout
        with io.StringIO() as text_trap:
            sys.stdout = text_trap
            with self.assertRaises(SystemExit) as cm:
                fsl_sub.cmdline.update(args=[])
            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '''Available updates:
fsl_sub (1.0.0 -> 2.0.0)
'''
            )
        mock_cup.assert_called_with(
            fsldir='/usr/local/fsl',
            packages=['fsl_sub', 'fsl_sub_plugin_sge', ]
        )
        self.assertEqual(
            'Aborted',
            str(cm.exception))
        mock_input.assert_called_once_with('Install pending updates? ')
        mock_input.reset_mock()

        mock_cup.reset_mock()
        mock_input.return_value = 'anythin'
        # Trap stdout
        with io.StringIO() as text_trap:
            sys.stdout = text_trap
            with self.assertRaises(SystemExit) as cm:
                fsl_sub.cmdline.update(args=[])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '''Available updates:
fsl_sub (1.0.0 -> 2.0.0)
'''
            )
        self.assertEqual(
            'Aborted',
            str(cm.exception))
        mock_cup.assert_called_with(
            fsldir='/usr/local/fsl',
            packages=['fsl_sub', 'fsl_sub_plugin_sge', ]
        )

        mock_cup.reset_mock()


@patch(
    'fsl_sub.cmdline.find_fsldir', autospec=True,
    return_value='/usr/local/fsl'
)
@patch(
    'fsl_sub.cmdline.conda_find_packages', autospec=True
)
@patch(
    'fsl_sub.cmdline.available_plugin_packages', autospec=True,
    return_value=['fsl_sub_plugin_sge', ]
)
class TestInstall(unittest.TestCase):
    def test_install_plugin_list(
            self, mock_pp,
            mock_fp, mock_fsldir):

        mock_fp.return_value = ['fsl_sub_plugin_sge', ]
        # Trap stdout
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            with self.assertRaises(SystemExit):
                fsl_sub.cmdline.install_plugin(args=['-l'])
            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '''Available plugins:
fsl_sub_plugin_sge
'''
            )
        mock_fp.assert_called_with(
            'fsl_sub_plugin_*',
            fsldir='/usr/local/fsl',
        )

    @patch(
        'fsl_sub.cmdline.conda_install', autospec=True)
    def test_list_and_install(
            self, mock_ci, mock_pp,
            mock_fp, mock_fsldir):

        mock_fp.return_value = ['fsl_sub_plugin_sge', ]
        mock_ci.return_value = {
            'fsl_sub_plugin_sge': {'version': '1.0.0', }}
        # Trap stdout
        with patch('fsl_sub.cmdline.user_input', autospec=True) as ui:
            ui.return_value = '1'
            with io.StringIO() as text_trap:
                sys.stdout = text_trap

                fsl_sub.cmdline.install_plugin(args=[])
                sys.stdout = sys.__stdout__

                self.assertEqual(
                    text_trap.getvalue(),
                    '''Available plugins:
1: fsl_sub_plugin_sge
Plugin fsl_sub_plugin_sge installed
You can generate an example config file with:
fsl_sub_config sge

The configuration file can be copied to /usr/local/fsl/etc/fslconf calling "
it fsl_sub.yml, or put in your home folder calling it .fsl_sub.yml. "
A copy in your home folder will override the file in "
/usr/local/fsl/etc/fslconf. Finally, the environment variable FSLSUB_CONF "
can be set to point at the configuration file, this will override all"
other files.
'''
                )
            mock_fp.assert_called_with(
                'fsl_sub_plugin_*',
                fsldir='/usr/local/fsl',
            )
            mock_ci.assert_called_once_with(
                'fsl_sub_plugin_sge'
            )
            ui.assert_called_once_with("Which backend? ")

    @patch(
        'fsl_sub.cmdline.conda_install', autospec=True)
    def test_list_and_install_badchoice(
            self, mock_ci, mock_pp,
            mock_fp, mock_fsldir):

        mock_fp.return_value = ['fsl_sub_plugin_sge', ]
        mock_ci.return_value = {
            'fsl_sub_plugin_sge': {'version': '1.0.0', }}
        # Trap stdout
        with patch('fsl_sub.cmdline.user_input', autospec=True) as ui:
            ui.return_value = '2'
            with io.StringIO() as text_trap:
                sys.stdout = text_trap

                with self.assertRaises(SystemExit) as se:
                    fsl_sub.cmdline.install_plugin(args=[])
                    self.assertEqual(
                        str(se.exception),
                        'Invalid plugin number')
                sys.stdout = sys.__stdout__

                self.assertEqual(
                    text_trap.getvalue(),
                    '''Available plugins:
1: fsl_sub_plugin_sge
'''
                )
            mock_fp.assert_called_with(
                'fsl_sub_plugin_*',
                fsldir='/usr/local/fsl',
            )
            ui.assert_called_once_with("Which backend? ")

    @patch(
        'fsl_sub.cmdline.conda_install', autospec=True)
    def test_install_direct(
            self, mock_ci, mock_pp,
            mock_fp, mock_fsldir):

        mock_fp.return_value = ['fsl_sub_plugin_sge', ]
        mock_ci.return_value = {
            'fsl_sub_plugin_sge': {'version': '1.0.0', }}
        # Trap stdout
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmdline.install_plugin(
                args=['-i', 'fsl_sub_plugin_sge'])
            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '''Plugin fsl_sub_plugin_sge installed
You can generate an example config file with:
fsl_sub_config sge

The configuration file can be copied to /usr/local/fsl/etc/fslconf calling "
it fsl_sub.yml, or put in your home folder calling it .fsl_sub.yml. "
A copy in your home folder will override the file in "
/usr/local/fsl/etc/fslconf. Finally, the environment variable FSLSUB_CONF "
can be set to point at the configuration file, this will override all"
other files.
'''
            )
        mock_fp.assert_called_with(
            'fsl_sub_plugin_*',
            fsldir='/usr/local/fsl',
        )
        mock_ci.assert_called_once_with(
            'fsl_sub_plugin_sge'
        )

    @patch(
        'fsl_sub.cmdline.conda_install', autospec=True)
    def test_install_direct_bad(
            self, mock_ci, mock_pp,
            mock_fp, mock_fsldir):

        mock_fp.return_value = ['fsl_sub_plugin_sge', ]
        mock_ci.return_value = {
            'fsl_sub_plugin_sge': {'version': '1.0.0', }}
        # Trap stdout
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            with self.assertRaises(SystemExit) as se:
                fsl_sub.cmdline.install_plugin(
                    args=['-i', 'fsl_sub_plugin_slurm'])
                self.assertEqual(
                    'Unrecognised plugin',
                    str(se.exception)
                )
            sys.stdout = sys.__stdout__

        mock_fp.assert_called_with(
            'fsl_sub_plugin_*',
            fsldir='/usr/local/fsl',
        )

# Once main tests are written they will need:


# @patch(
#     'fsl_sub.cmdline.load_plugins',
#     autospec=True,
#     return_value=FakePlugin()
#     )
