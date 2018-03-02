#!/usr/bin/env python
import subprocess
import unittest
import xml.etree.ElementTree as ET
import yaml
import fsl_sub.plugins.fsl_sub_SGE

from unittest.mock import (patch, mock_open, call)


class TestSgeFinders(unittest.TestCase):
    @patch('fsl_sub.plugins.fsl_sub_SGE.qconf', autospec=True)
    def test_qtest(self, mock_qconf):
        bin_path = '/opt/sge/bin/qconf'
        mock_qconf.return_value = bin_path
        self.assertEqual(
            bin_path,
            fsl_sub.plugins.fsl_sub_SGE.qtest()
        )
        mock_qconf.assert_called_once_with()

    @patch('fsl_sub.plugins.fsl_sub_SGE.which', autospec=True)
    def test_qconf(self, mock_which):
        bin_path = '/opt/sge/bin/qconf'
        with self.subTest("Test 1"):
            mock_which.return_value = bin_path
            self.assertEqual(
                bin_path,
                fsl_sub.plugins.fsl_sub_SGE.qconf()
            )
        mock_which.reset()
        with self.subTest("Test 2"):
            mock_which.return_value = None
            self.assertRaises(
                fsl_sub.plugins.fsl_sub_SGE.BadSubmission,
                fsl_sub.plugins.fsl_sub_SGE.qconf
            )

    @patch('fsl_sub.plugins.fsl_sub_SGE.which', autospec=True)
    def test_qstat(self, mock_which):
        bin_path = '/opt/sge/bin/qstat'
        with self.subTest("Test 1"):
            mock_which.return_value = bin_path
            self.assertEqual(
                bin_path,
                fsl_sub.plugins.fsl_sub_SGE.qstat()
            )
        mock_which.reset()
        with self.subTest("Test 2"):
            mock_which.return_value = None
            self.assertRaises(
                fsl_sub.plugins.fsl_sub_SGE.BadSubmission,
                fsl_sub.plugins.fsl_sub_SGE.qstat
            )

    @patch('fsl_sub.plugins.fsl_sub_SGE.which', autospec=True)
    def qsub(self, mock_which):
        bin_path = '/opt/sge/bin/qsub'
        with self.subTest("Test 1"):
            mock_which.return_value = bin_path
            self.assertEqual(
                bin_path,
                fsl_sub.plugins.fsl_sub_SGE.qsub()
            )
        mock_which.reset()
        with self.subTest("Test 2"):
            mock_which.return_value = None
            self.assertRaises(
                fsl_sub.plugins.fsl_sub_SGE.BadSubmission,
                fsl_sub.plugins.fsl_sub_SGE.qsub
            )

    @patch('fsl_sub.plugins.fsl_sub_SGE.qconf', autospec=True)
    @patch('fsl_sub.plugins.fsl_sub_SGE.sp.run', autospec=True)
    def queue_exists(self, mock_spr, mock_qconf):
        bin_path = '/opt/sge/bin/qtest'
        qname = 'myq'
        with self.subTest("Test 1"):
            mock_qconf.return_value = bin_path
            mock_spr.return_value = subprocess.CompletedProcess(
                [bin_path, '-sq', qname],
                returncode=0
            )
            self.assertTrue(
                fsl_sub.plugins.fsl_sub_SGE.queue_exists(qname)
            )
            mock_qconf.assert_called_once_with()
            mock_spr.assert_called_once_with(
                [bin_path, '-sq', qname],
                stderr=subprocess.PIPE,
                check=True,
                universal_newlines=True)
        mock_qconf.reset()
        with self.subTest("Test 2"):
            mock_qconf.side_effect = fsl_sub.plugins.fsl_sub_SGE.BadSubmission(
                "Bad")
            self.assertRaises(
                fsl_sub.plugins.fsl_sub_SGE.BadSubmission,
                fsl_sub.plugins.fsl_sub_SGE.queue_exists,
                qname
            )
        mock_qconf.reset()
        with self.subTest("Test 3"):
            self.assertTrue(
                fsl_sub.plugins.fsl_sub_SGE.queue_exists(qname, bin_path)
            )
            self.assertFalse(mock_qconf.called)
        mock_spr.reset()
        with self.subTest("Test 4"):
            mock_spr.side_effect = subprocess.CalledProcess_error()
            self.assertFalse(
                fsl_sub.plugins.fsl_sub_SGE.queue_exists(qname, bin_path)
            )


@patch(
    'fsl_sub.plugins.fsl_sub_SGE.qconf',
    autospec=True, return_value="/usr/bin/qconf")
@patch(
    'fsl_sub.plugins.fsl_sub_SGE.qstat',
    autospec=True, return_value="/usr/bin/qstat")
@patch('fsl_sub.plugins.fsl_sub_SGE.sp.run', autospec=True)
class TestCheckPE(unittest.TestCase):
    def test_queue_name(self, mock_spr, mock_qstat, mock_qconf):
        mock_spr.return_value = subprocess.CompletedProcess('a', 1)
        self.assertRaises(
            fsl_sub.plugins.fsl_sub_SGE.BadSubmission,
            fsl_sub.plugins.fsl_sub_SGE.check_pe,
            "nope", "aqueue"
        )
        mock_spr.assert_called_once_with(
            ["/usr/bin/qconf", "-sp", "nope"]
        )
        mock_qstat.assert_called_once_with()
        mock_qconf.assert_called_once_with()

    def test_pe_available_anywhere(self, mock_spr, mock_qstat, mock_qconf):
        with self.subTest("SonOfGrid Engine"):
            mock_spr.side_effect = [
                subprocess.CompletedProcess('a', 0),
                subprocess.CompletedProcess('a', 1)
            ]
            self.assertRaises(
                fsl_sub.plugins.fsl_sub_SGE.BadSubmission,
                fsl_sub.plugins.fsl_sub_SGE.check_pe,
                "ape", "aqueue"
            )
        with self.subTest("Univa Grid Engine"):
            mock_spr.side_effect = [
                subprocess.CompletedProcess('a', 0),
                subprocess.CompletedProcess(
                    'a', 0,
                    stderr="error: no such parallel environment")
            ]
            self.assertRaises(
                fsl_sub.plugins.fsl_sub_SGE.BadSubmission,
                fsl_sub.plugins.fsl_sub_SGE.check_pe,
                "ape", "aqueue"
            )

    def test_pe_available(self, mock_spr, mock_qstat, mock_qconf):
        with patch(
                'fsl_sub.plugins.fsl_sub_SGE.ET.fromstring',
                autospec=True) as me:
            example_pe_xml = '''<?xml version='1.0'?>
<job_info  xmlns:xsd="http://a.web.server.com/qstat.xsd">
  <cluster_queue_summary>
    <name>a.q</name>
    <load>0.44016</load>
    <used>0</used>
    <resv>0</resv>
    <available>1</available>
    <total>1</total>
    <temp_disabled>0</temp_disabled>
    <manual_intervention>0</manual_intervention>
  </cluster_queue_summary>
</job_info>
'''
            me.return_value = ET.fromstring(example_pe_xml)
            mock_spr.side_effect = [
                subprocess.CompletedProcess('a', 0),
                subprocess.CompletedProcess(
                    'a', 0,
                    stdout=example_pe_xml),
            ]
            self.assertRaises(
                fsl_sub.plugins.fsl_sub_SGE.BadSubmission,
                fsl_sub.plugins.fsl_sub_SGE.check_pe,
                "ape", "b.q"
            )
            mock_spr.reset()
            me.reset()
            self.assertRaises(
                fsl_sub.plugins.fsl_sub_SGE.BadSubmission,
                fsl_sub.plugins.fsl_sub_SGE.check_pe,
                "ape", "ba.q"
            )
            mock_spr.reset()
            me.reset()
            self.assertRaises(
                fsl_sub.plugins.fsl_sub_SGE.BadSubmission,
                fsl_sub.plugins.fsl_sub_SGE.check_pe,
                "ape", "a.q1"
            )
            mock_spr.reset()
            me.reset()
            fsl_sub.plugins.fsl_sub_SGE.check_pe("ape", "a.q")
            me.assert_called_once_with(example_pe_xml)


@patch(
    'fsl_sub.plugins.fsl_sub_SGE.shlex.split',
    autospec=True)
@patch(
    'fsl_sub.plugins.fsl_sub_SGE.check_pe',
    autospec=True
)
@patch(
    'fsl_sub.plugins.fsl_sub_SGE.os.getcwd',
    autospec=True, return_value='/Users/testuser')
@patch(
    'fsl_sub.plugins.fsl_sub_SGE.qsub',
    autospec=True, return_value='/usr/bin/qsub'
)
@patch('fsl_sub.plugins.fsl_sub_SGE.method_config', autospec=True)
@patch('fsl_sub.plugins.fsl_sub_SGE.split_ram_by_slots', autospec=True)
@patch('fsl_sub.plugins.fsl_sub_SGE.copro_conf', autospec=True)
@patch(
    'fsl_sub.plugins.fsl_sub_SGE.tempfile.NamedTemporaryFile',
    autospec=True)
@patch('fsl_sub.plugins.fsl_sub_SGE.sp.run', autospec=True)
class TestSubmit(unittest.TestCase):
    mconf_dict = yaml.load('''---
SGE:
    parallel_envs:
      - shmem
    same_node_pes:
      - shmem
    large_job_split_pe: shmem
    copy_environment: True
    affinity_type: linear
    affinity_control: threads
    mail_support: True
    mail_modes:
      - b
      - e
      - a
      - s
      - n
    mail_mode: a
    map_ram: True
    ram_resources:
        - m_mem_free
        - h_vmem
    ram_units: G
    job_priorities: True
    max_priority: 1023
    parallel_holds: True
    parallel_limit: True
    architecture: False
    job_resources: True
copro_opts:
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
''')
    plugin = fsl_sub.plugins.fsl_sub_SGE

    def test_empty_submit(
            self, mock_sprun, mock_ntf, mock_cpconf,
            mock_srbs, mock_mconf, mock_qsub,
            mock_getcwd, mock_check_pe, mock_shlex):
        self.assertRaises(
            self.plugin.BadSubmission,
            self.plugin.submit,
            None, None, None
        )

    def test_submit_basic(
            self, mock_sprun, mock_ntf, mock_cpconf,
            mock_srbs, mock_mconf, mock_qsub,
            mock_getcwd, mock_check_pe, mock_shlex):
        mock_mconf.return_value = self.mconf_dict
        job_name = 'test_job'
        queue = 'a.q'
        cmd = ['acmd', 'arg1', 'arg2', ]

        jid = 12345
        qsub_out = 'Your job ' + jid + ' ("acmd") has been submitted'
        with self.subTest("Univa"):
            expected_cmd = [
                '/usr/bin/qsub',
                '-V',
                '-binding',
                'linear:1',
                '-o', '/Users/testuser',
                '-e', '/Users/testuser',
                '-N', 'test_job',
                '-cwd', '-q', 'a.q',
                '-shell', 'n',
                '-b', 'y',
                '-r', 'y',
                'acmd', 'arg1', 'arg2'
            ]
            self.mock_sprun.return_value = subprocess.CompletedProcess(
                expected_cmd, 0, stdout=qsub_out, stderr=None)
            self.assertEqual(
                jid,
                self.plugin.submit(
                    command=cmd,
                    job_name=job_name,
                    queue=queue
                )
            )
            self.mock_sprun.assert_called_once_with(
                expected_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
        self.mock_sprun.reset()
        with self.subTest("SGE"):
            sge_dict = dict(self.mconf_dict)
            sge_dict['affinity_control'] = 'slots'
            mock_mconf.return_value = sge_dict
            expected_cmd = [
                    '/usr/bin/qsub',
                    '-V',
                    '-binding',
                    'linear:slots',
                    '-o', '/Users/testuser',
                    '-e', '/Users/testuser',
                    '-N', 'test_job',
                    '-cwd', '-q', 'a.q',
                    '-shell', 'n',
                    '-b', 'y',
                    '-r', 'y',
                    'acmd', 'arg1', 'arg2'
                ]
            self.mock_sprun.return_value = subprocess.CompletedProcess(
                expected_cmd, 0, stdout=qsub_out, stderr=None)
            self.assertEqual(
                jid,
                self.plugin.submit(
                    command=cmd,
                    job_name=job_name,
                    queue=queue
                )
            )
            self.mock_sprun.assert_called_once_with(
                expected_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
        self.mock_sprun.reset()
        with self.subTest("Bad affinity type"):
            sge_dict['affinity_control'] = 'nonsense'
            mock_mconf.return_value = sge_dict
            self.assertRaises(
                self.plugin.BadConfiguration,
                self.plugin.submit,
                command=cmd,
                job_name=job_name,
                queue=queue
            )

    def test_no_env_submit(
            self, mock_sprun, mock_ntf, mock_cpconf,
            mock_srbs, mock_mconf, mock_qsub,
            mock_getcwd, mock_check_pe, mock_shlex):
        test_dict = dict(self.mconf_dict)
        test_dict['copy_environment'] = False
        mock_mconf.return_value = test_dict
        job_name = 'test_job'
        queue = 'a.q'
        cmd = ['acmd', 'arg1', 'arg2', ]

        jid = 12345
        qsub_out = 'Your job ' + jid + ' ("acmd") has been submitted'
        expected_cmd = [
            '/usr/bin/qsub',
            '-binding',
            'linear:1',
            '-o', '/Users/testuser',
            '-e', '/Users/testuser',
            '-N', 'test_job',
            '-cwd', '-q', 'a.q',
            '-shell', 'n',
            '-b', 'y',
            '-r', 'y',
            'acmd', 'arg1', 'arg2'
        ]
        self.mock_sprun.return_value = subprocess.CompletedProcess(
            expected_cmd, 0, stdout=qsub_out, stderr=None)
        self.assertEqual(
            jid,
            self.plugin.submit(
                command=cmd,
                job_name=job_name,
                queue=queue,
                )
        )
        self.mock_sprun.assert_called_once_with(
            expected_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

    def test_no_affinity_submit(
            self, mock_sprun, mock_ntf, mock_cpconf,
            mock_srbs, mock_mconf, mock_qsub,
            mock_getcwd, mock_check_pe, mock_shlex):
        test_dict = dict(self.mconf_dict)
        test_dict['affinity_type'] = None
        mock_mconf.return_value = test_dict
        job_name = 'test_job'
        queue = 'a.q'
        cmd = ['acmd', 'arg1', 'arg2', ]

        jid = 12345
        qsub_out = 'Your job ' + jid + ' ("acmd") has been submitted'
        expected_cmd = [
            '/usr/bin/qsub',
            '-V',
            '-o', '/Users/testuser',
            '-e', '/Users/testuser',
            '-N', 'test_job',
            '-cwd', '-q', 'a.q',
            '-shell', 'n',
            '-b', 'y',
            '-r', 'y',
            'acmd', 'arg1', 'arg2'
        ]
        self.mock_sprun.return_value = subprocess.CompletedProcess(
            expected_cmd, 0, stdout=qsub_out, stderr=None)
        self.assertEqual(
            jid,
            self.plugin.submit(
                command=cmd,
                job_name=job_name,
                queue=queue,
                )
        )
        self.mock_sprun.assert_called_once_with(
                expected_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

    def test_priority_submit(
            self, mock_sprun, mock_ntf, mock_cpconf,
            mock_srbs, mock_mconf, mock_qsub,
            mock_getcwd, mock_check_pe, mock_shlex):
            job_name = 'test_job'
            queue = 'a.q'
            cmd = ['acmd', 'arg1', 'arg2', ]

            jid = 12345
            qsub_out = 'Your job ' + jid + ' ("acmd") has been submitted'
            with self.subTest("No priorities"):
                test_dict = dict(self.mconf_dict)
                test_dict['job_priorities'] = False
                mock_mconf.return_value = test_dict
                expected_cmd = [
                    '/usr/bin/qsub',
                    '-V',
                    '-binding',
                    'linear:1',
                    '-o', '/Users/testuser',
                    '-e', '/Users/testuser',
                    '-N', 'test_job',
                    '-cwd', '-q', 'a.q',
                    '-shell', 'n',
                    '-b', 'y',
                    '-r', 'y',
                    'acmd', 'arg1', 'arg2'
                ]
                self.mock_sprun.return_value = subprocess.CompletedProcess(
                    expected_cmd, 0, stdout=qsub_out, stderr=None)
                self.assertEqual(
                    jid,
                    self.plugin.submit(
                        command=cmd,
                        job_name=job_name,
                        queue=queue,
                        priority=1000
                        )
                )
                self.mock_sprun.assert_called_once_with(
                    expected_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
            self.mock_sprun.reset()
            mock_mconf.return_value = self.mconf_dict
            with self.subTest("With priorities"):
                test_dict = dict(self.mconf_dict)
                test_dict['job_priorities'] = False
                mock_mconf.return_value = test_dict
                expected_cmd = [
                    '/usr/bin/qsub',
                    '-V',
                    '-binding',
                    'linear:1',
                    '-p', 1000,
                    '-o', '/Users/testuser',
                    '-e', '/Users/testuser',
                    '-N', 'test_job',
                    '-cwd', '-q', 'a.q',
                    '-shell', 'n',
                    '-b', 'y',
                    '-r', 'y',
                    'acmd', 'arg1', 'arg2'
                ]
                self.mock_sprun.return_value = subprocess.CompletedProcess(
                    expected_cmd, 0, stdout=qsub_out, stderr=None)
                self.assertEqual(
                    jid,
                    self.plugin.submit(
                        command=cmd,
                        job_name=job_name,
                        queue=queue,
                        priority=1000
                        )
                )
                self.mock_sprun.assert_called_once_with(
                    expected_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
            self.mock_sprun.reset()
            mock_mconf.return_value = self.mconf_dict
            with self.subTest("With priorities (limited)"):
                test_dict = dict(self.mconf_dict)
                test_dict['job_priorities'] = False
                mock_mconf.return_value = test_dict
                expected_cmd = [
                    '/usr/bin/qsub',
                    '-V',
                    '-binding',
                    'linear:1',
                    '-p', 1023,
                    '-o', '/Users/testuser',
                    '-e', '/Users/testuser',
                    '-N', 'test_job',
                    '-cwd', '-q', 'a.q',
                    '-shell', 'n',
                    '-b', 'y',
                    '-r', 'y',
                    'acmd', 'arg1', 'arg2'
                ]
                self.mock_sprun.return_value = subprocess.CompletedProcess(
                    expected_cmd, 0, stdout=qsub_out, stderr=None)
                self.assertEqual(
                    jid,
                    self.plugin.submit(
                        command=cmd,
                        job_name=job_name,
                        queue=queue,
                        priority=2000
                        )
                )
                self.mock_sprun.assert_called_once_with(
                    expected_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )

    def test_resources_submit(
            self, mock_sprun, mock_ntf, mock_cpconf,
            mock_srbs, mock_mconf, mock_qsub,
            mock_getcwd, mock_check_pe, mock_shlex):
            job_name = 'test_job'
            queue = 'a.q'
            cmd = ['acmd', 'arg1', 'arg2', ]

            jid = 12345
            qsub_out = 'Your job ' + jid + ' ("acmd") has been submitted'
            mock_mconf.return_value = self.mconf_dict
            with self.subTest("With single resource"):
                expected_cmd = [
                    '/usr/bin/qsub',
                    '-V',
                    '-binding',
                    'linear:1',
                    '-l', 'ramlimit=1000',
                    '-o', '/Users/testuser',
                    '-e', '/Users/testuser',
                    '-N', 'test_job',
                    '-cwd', '-q', 'a.q',
                    '-shell', 'n',
                    '-b', 'y',
                    '-r', 'y',
                    'acmd', 'arg1', 'arg2'
                ]
                self.mock_sprun.return_value = subprocess.CompletedProcess(
                    expected_cmd, 0, stdout=qsub_out, stderr=None)
                self.assertEqual(
                    jid,
                    self.plugin.submit(
                        command=cmd,
                        job_name=job_name,
                        queue=queue,
                        resources='ramlimit=1000'
                        )
                )
                self.mock_sprun.assert_called_once_with(
                    expected_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
            self.mock_sprun.reset()
            with self.subTest("With multiple resources"):
                expected_cmd = [
                    '/usr/bin/qsub',
                    '-V',
                    '-binding',
                    'linear:1',
                    '-l', 'resource1=1,resource2=2',
                    '-o', '/Users/testuser',
                    '-e', '/Users/testuser',
                    '-N', 'test_job',
                    '-cwd', '-q', 'a.q',
                    '-shell', 'n',
                    '-b', 'y',
                    '-r', 'y',
                    'acmd', 'arg1', 'arg2'
                ]
                self.mock_sprun.return_value = subprocess.CompletedProcess(
                    expected_cmd, 0, stdout=qsub_out, stderr=None)
                self.assertEqual(
                    jid,
                    self.plugin.submit(
                        command=cmd,
                        job_name=job_name,
                        queue=queue,
                        resources=['resource1=1', 'resource2=2', ]
                        )
                )
                self.mock_sprun.assert_called_once_with(
                    expected_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )

    def test_job_hold_submit(
            self, mock_sprun, mock_ntf, mock_cpconf,
            mock_srbs, mock_mconf, mock_qsub,
            mock_getcwd, mock_check_pe, mock_shlex):
        job_name = 'test_job'
        queue = 'a.q'
        cmd = ['acmd', 'arg1', 'arg2', ]

        jid = 12345
        hjid = 12344
        qsub_out = 'Your job ' + jid + ' ("acmd") has been submitted'
        mock_mconf.return_value = self.mconf_dict
        expected_cmd = [
            '/usr/bin/qsub',
            '-V',
            '-binding',
            'linear:1',
            '-o', '/Users/testuser',
            '-e', '/Users/testuser',
            '-hold_jid', hjid,
            '-N', 'test_job',
            '-cwd', '-q', 'a.q',
            '-shell', 'n',
            '-b', 'y',
            '-r', 'y',
            'acmd', 'arg1', 'arg2'
        ]
        self.mock_sprun.return_value = subprocess.CompletedProcess(
            expected_cmd, 0, stdout=qsub_out, stderr=None)
        self.assertEqual(
            jid,
            self.plugin.submit(
                command=cmd,
                job_name=job_name,
                queue=queue,
                jobhold=hjid
                )
        )
        self.mock_sprun.assert_called_once_with(
            expected_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

    def test_no_array_hold_submit(
            self, mock_sprun, mock_ntf, mock_cpconf,
            mock_srbs, mock_mconf, mock_qsub,
            mock_getcwd, mock_check_pe, mock_shlex):
        job_name = 'test_job'
        queue = 'a.q'
        cmd = ['acmd', 'arg1', 'arg2', ]

        mock_mconf.return_value = self.mconf_dict
        self.assertRases(
            self.plugin.BadSubmission,
            self.plugin.submit,
            command=cmd,
            job_name=job_name,
            queue=queue,
            array_hold=12345
        )

    def test_no_array_limit_submit(
            self, mock_sprun, mock_ntf, mock_cpconf,
            mock_srbs, mock_mconf, mock_qsub,
            mock_getcwd, mock_check_pe, mock_shlex):
        job_name = 'test_job'
        queue = 'a.q'
        cmd = ['acmd', 'arg1', 'arg2', ]

        mock_mconf.return_value = self.mconf_dict
        self.assertRases(
            self.plugin.BadSubmission,
            self.plugin.submit,
            command=cmd,
            job_name=job_name,
            queue=queue,
            array_limit=5
        )

    def test_jobram_submit(
            self, mock_sprun, mock_ntf, mock_cpconf,
            mock_srbs, mock_mconf, mock_qsub,
            mock_getcwd, mock_check_pe, mock_shlex):
        job_name = 'test_job'
        queue = 'a.q'
        jid = 123456
        jobram = 1024
        cmd = ['acmd', 'arg1', 'arg2', ]

        mock_mconf.return_value = self.mconf_dict
        qsub_out = 'Your job ' + jid + ' ("acmd") has been submitted'
        expected_cmd = [
            '/usr/bin/qsub',
            '-V',
            '-binding',
            'linear:1',
            '-o', '/Users/testuser',
            '-e', '/Users/testuser',
            '-l', "m_mem_free={}G,h_vmem={}G".format(jobram),
            '-N', 'test_job',
            '-cwd', '-q', 'a.q',
            '-shell', 'n',
            '-b', 'y',
            '-r', 'y',
            'acmd', 'arg1', 'arg2'
        ]
        self.mock_sprun.return_value = subprocess.CompletedProcess(
            expected_cmd, 0, stdout=qsub_out, stderr=None)
        self.assertEqual(
            jid,
            self.plugin.submit(
                command=cmd,
                job_name=job_name,
                queue=queue,
                jobram=jobram
                )
        )
        self.mock_sprun.assert_called_once_with(
            expected_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

    def test_mail_support(
            self, mock_sprun, mock_ntf, mock_cpconf,
            mock_srbs, mock_mconf, mock_qsub,
            mock_getcwd, mock_check_pe, mock_shlex):
        job_name = 'test_job'
        queue = 'a.q'
        jid = 123456
        mailto = 'auser@adomain.com'
        mailon = 'e'
        cmd = ['acmd', 'arg1', 'arg2', ]

        mock_mconf.return_value = self.mconf_dict
        qsub_out = 'Your job ' + jid + ' ("acmd") has been submitted'
        with self.subTest("Test mail settings"):
            expected_cmd = [
                '/usr/bin/qsub',
                '-V',
                '-binding',
                'linear:1',
                '-o', '/Users/testuser',
                '-e', '/Users/testuser',
                '-M', mailto,
                '-m', mailon,
                '-N', 'test_job',
                '-cwd', '-q', 'a.q',
                '-shell', 'n',
                '-b', 'y',
                '-r', 'y',
                'acmd', 'arg1', 'arg2'
            ]
            self.mock_sprun.return_value = subprocess.CompletedProcess(
                expected_cmd, 0, stdout=qsub_out, stderr=None)
            self.assertEqual(
                jid,
                self.plugin.submit(
                    command=cmd,
                    job_name=job_name,
                    queue=queue,
                    mailto=mailto,
                    mailon=mailon
                    )
            )
            self.mock_sprun.assert_called_once_with(
                expected_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
        self.mock_sprun.reset()
        with self.subTest("Test for auto set of mail mode"):
            expected_cmd = [
                '/usr/bin/qsub',
                '-V',
                '-binding',
                'linear:1',
                '-o', '/Users/testuser',
                '-e', '/Users/testuser',
                '-M', mailto,
                '-m', self.mconf['mail_mode'],
                '-N', 'test_job',
                '-cwd', '-q', 'a.q',
                '-shell', 'n',
                '-b', 'y',
                '-r', 'y',
                'acmd', 'arg1', 'arg2'
            ]
            self.mock_sprun.return_value = subprocess.CompletedProcess(
                expected_cmd, 0, stdout=qsub_out, stderr=None)
            self.assertEqual(
                jid,
                self.plugin.submit(
                    command=cmd,
                    job_name=job_name,
                    queue=queue,
                    mailto=mailto,
                    )
            )
            self.mock_sprun.assert_called_once_with(
                expected_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
        with self.subTest("Test for bad input"):
            mailon = 't'
            self.assertRaises(
                self.plugin.BadSubmission,
                self.plugin.submit,
                command=cmd,
                job_name=job_name,
                queue=queue,
                mailto=mailto,
                mailon=mailon
            )

    def test_coprocessor_submit(
            self, mock_sprun, mock_ntf, mock_cpconf,
            mock_srbs, mock_mconf, mock_qsub,
            mock_getcwd, mock_check_pe, mock_shlex):
        job_name = 'test_job'
        queue = 'a.q'
        jid = 123456
        cmd = ['acmd', 'arg1', 'arg2', ]
        copro_type = 'cuda'
        copro_opts = self.mconf_dict['copro_opts'][copro_type]
        gpuclass = copro_opts['P']
        gputype = copro_opts['class_types'][gpuclass]['resource']
        mock_mconf.return_value = self.mconf_dict
        qsub_out = 'Your job ' + jid + ' ("acmd") has been submitted'
        with self.subTest("Test basic GPU"):
            expected_cmd = [
                '/usr/bin/qsub',
                '-V',
                '-binding',
                'linear:1',
                '-o', '/Users/testuser',
                '-e', '/Users/testuser',
                '-N', 'test_job',
                '-cwd', '-q', 'a.q',
                '-l', 'gputype=' + gputype,
                '-l', 'gpu=1',
                '-shell', 'n',
                '-b', 'y',
                '-r', 'y',
                'acmd', 'arg1', 'arg2'
            ]
            self.mock_sprun.return_value = subprocess.CompletedProcess(
                expected_cmd, 0, stdout=qsub_out, stderr=None)
            self.assertEqual(
                jid,
                self.plugin.submit(
                    command=cmd,
                    job_name=job_name,
                    queue=queue,
                    coprocessor=copro_type,
                    )
            )
            self.mock_sprun.assert_called_once_with(
                expected_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
        self.mock_sprun.reset()
        with self.subTest("Test specific class of GPU"):
            gpuclass = 'K'
            gputype = copro_opts['class_types'][gpuclass]['resource']
            expected_cmd = [
                '/usr/bin/qsub',
                '-V',
                '-binding',
                'linear:1',
                '-o', '/Users/testuser',
                '-e', '/Users/testuser',
                '-N', 'test_job',
                '-cwd', '-q', 'a.q',
                '-l', 'gputype=' + gputype,
                '-l', 'gpu=1',
                '-shell', 'n',
                '-b', 'y',
                '-r', 'y',
                'acmd', 'arg1', 'arg2'
            ]
            self.mock_sprun.return_value = subprocess.CompletedProcess(
                expected_cmd, 0, stdout=qsub_out, stderr=None)
            self.assertEqual(
                jid,
                self.plugin.submit(
                    command=cmd,
                    job_name=job_name,
                    queue=queue,
                    coprocessor=copro_type,
                    coprocessor_class_strict=True
                    )
            )
            self.mock_sprun.assert_called_once_with(
                expected_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
        self.mock_sprun.reset()
        with self.subTest("Test more capable classes of GPU"):
            gpuclass = 'K'
            gputype = copro_opts['class_types'][gpuclass]['resource']
            second_gtype = copro_opts['class_types']['P']['resource']
            expected_cmd = [
                '/usr/bin/qsub',
                '-V',
                '-binding',
                'linear:1',
                '-o', '/Users/testuser',
                '-e', '/Users/testuser',
                '-N', 'test_job',
                '-cwd', '-q', 'a.q',
                '-l', 'gputype={0},{1}'.format(gputype, second_gtype),
                '-l', 'gpu=1',
                '-shell', 'n',
                '-b', 'y',
                '-r', 'y',
                'acmd', 'arg1', 'arg2'
            ]
            self.mock_sprun.return_value = subprocess.CompletedProcess(
                expected_cmd, 0, stdout=qsub_out, stderr=None)
            self.assertEqual(
                jid,
                self.plugin.submit(
                    command=cmd,
                    job_name=job_name,
                    queue=queue,
                    coprocessor=copro_type,
                    )
            )
            self.mock_sprun.assert_called_once_with(
                expected_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
        self.mock_sprun.reset()
        with self.subTest("Test more capable classes of GPU (configuration)"):
            test_mconf = dict(self.mconf_dict)
            test_mconf['include_mode_capable'] = False
            gpuclass = 'K'
            gputype = copro_opts['class_types'][gpuclass]['resource']
            second_gtype = copro_opts['class_types']['P']['resource']
            expected_cmd = [
                '/usr/bin/qsub',
                '-V',
                '-binding',
                'linear:1',
                '-o', '/Users/testuser',
                '-e', '/Users/testuser',
                '-N', 'test_job',
                '-cwd', '-q', 'a.q',
                '-l', 'gputype=' + gputype,
                '-l', 'gpu=1',
                '-shell', 'n',
                '-b', 'y',
                '-r', 'y',
                'acmd', 'arg1', 'arg2'
            ]
            mock_mconf.return_value = test_mconf
            self.mock_sprun.return_value = subprocess.CompletedProcess(
                expected_cmd, 0, stdout=qsub_out, stderr=None)
            self.assertEqual(
                jid,
                self.plugin.submit(
                    command=cmd,
                    job_name=job_name,
                    queue=queue,
                    coprocessor=copro_type,
                    )
            )
            self.mock_sprun.assert_called_once_with(
                expected_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
        self.mock_sprun.reset()
        with self.subTest("Test multi-GPU"):
            multi_gpu = 2
            gpuclass = copro_opts['default_class']
            gputype = copro_opts['class_types'][gpuclass]['resource']
            expected_cmd = [
                '/usr/bin/qsub',
                '-V',
                '-binding',
                'linear:1',
                '-o', '/Users/testuser',
                '-e', '/Users/testuser',
                '-N', 'test_job',
                '-cwd', '-q', 'a.q',
                '-l', 'gputype=' + gputype,
                '-l', 'gpu=' + multi_gpu,
                '-shell', 'n',
                '-b', 'y',
                '-r', 'y',
                'acmd', 'arg1', 'arg2'
            ]
            self.mock_sprun.return_value = subprocess.CompletedProcess(
                expected_cmd, 0, stdout=qsub_out, stderr=None)
            self.assertEqual(
                jid,
                self.plugin.submit(
                    command=cmd,
                    job_name=job_name,
                    queue=queue,
                    coprocessor=copro_type,
                    coprocessor_multi=2
                    )
            )
            self.mock_sprun.assert_called_once_with(
                expected_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

    def test_parallel_env_submit(
            self, mock_sprun, mock_ntf, mock_cpconf,
            mock_srbs, mock_mconf, mock_qsub,
            mock_getcwd, mock_check_pe, mock_shlex):
        mock_mconf.return_value = self.mconf_dict
        job_name = 'test_job'
        queue = 'a.q'
        cmd = ['acmd', 'arg1', 'arg2', ]

        jid = 12345
        qsub_out = 'Your job ' + jid + '("acmd") has been submitted'
        with self.subTest("One thread"):
            expected_cmd = [
                '/usr/bin/qsub',
                '-pe', 'openmp', 1, '-w', 'e',
                '-V',
                '-binding',
                'linear:1',
                '-o', '/Users/testuser',
                '-e', '/Users/testuser',
                '-N', 'test_job',
                '-cwd', '-q', 'a.q',
                '-shell', 'n',
                '-b', 'y',
                '-r', 'y',
                'acmd', 'arg1', 'arg2'
            ]
            self.mock_sprun.return_value = subprocess.CompletedProcess(
                expected_cmd, 0, stdout=qsub_out, stderr=None)
            self.assertEqual(
                jid,
                self.plugin.submit(
                    command=cmd,
                    job_name=job_name,
                    queue=queue,
                    parallel_env='openmp',
                )
            )
            self.mock_sprun.assert_called_once_with(
                expected_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
        with self.subTest("Two threads"):
            expected_cmd = [
                '/usr/bin/qsub',
                '-pe', 'openmp', 2, '-w', 'e',
                '-V',
                '-binding',
                'linear:1',
                '-o', '/Users/testuser',
                '-e', '/Users/testuser',
                '-N', 'test_job',
                '-cwd', '-q', 'a.q',
                '-shell', 'n',
                '-b', 'y',
                '-r', 'y',
                'acmd', 'arg1', 'arg2'
            ]
            self.mock_sprun.return_value = subprocess.CompletedProcess(
                expected_cmd, 0, stdout=qsub_out, stderr=None)
            self.assertEqual(
                jid,
                self.plugin.submit(
                    command=cmd,
                    job_name=job_name,
                    queue=queue,
                    parallel_env='openmp',
                    threads=2
                )
            )
            self.mock_sprun.assert_called_once_with(
                expected_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

        with self.subTest("Bad PE"):
            mock_check_pe.side_effect = self.plugin.BadSubmission
            self.assertRaises(
                self.plugin.BadSubmission,
                self.plugin.submit,
                command=cmd,
                job_name=job_name,
                queue=queue,
                parallel_env='openmp',
                threads=2
            )

    def test_array_hold_on_non_array_submit(
            self, mock_sprun, mock_ntf, mock_cpconf,
            mock_srbs, mock_mconf, mock_qsub,
            mock_getcwd, mock_check_pe, mock_shlex):
        job_name = 'test_job'
        queue = 'a.q'
        cmd = ['acmd', 'arg1', 'arg2', ]

        hjid = 12344
        mock_mconf.return_value = self.mconf_dict
        expected_cmd = [
            '/usr/bin/qsub',
            '-V',
            '-binding',
            'linear:1',
            '-o', '/Users/testuser',
            '-e', '/Users/testuser',
            '-hold_jid_ad', hjid,
            '-N', 'test_job',
            '-cwd', '-q', 'a.q',
            '-shell', 'n',
            '-b', 'y',
            '-r', 'y',
            'acmd', 'arg1', 'arg2'
        ]
        self.assertRases(
            self.plugin.BadSubmission,
            self.plugin.submit,
            command=cmd,
            job_name=job_name,
            queue=queue,
            array_hold=hjid
        )

        self.mock_sprun.assert_called_once_with(
            expected_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

    @patch('fsl_sub.plugins.fsl_sub_SGE.os.remove', autospec=True)
    def test_array_submit(
            self, mock_osr, mock_sprun, mock_ntf, mock_cpconf,
            mock_srbs, mock_mconf, mock_qsub,
            mock_getcwd, mock_check_pe, mock_shlex):
        job_name = 'test_job'
        queue = 'a.q'
        job_file = '''
acmd 1 2 3
acmd 4 5 6
acmd 6 7 8
'''
        job_file_name = 'll_job'
        tmp_file = 'atmpfile'
        jid = 12344
        qsub_out = 'Your job ' + jid + ' ("test_job") has been submitted'
        mock_mconf.return_value = self.mconf_dict
        expected_cmd = [
            '/usr/bin/qsub',
            '-V',
            '-binding',
            'linear:1',
            '-o', '/Users/testuser',
            '-e', '/Users/testuser',
            '-N', 'test_job',
            '-cwd', '-q', 'a.q',
            '-t', "1-4:1",
            'll_job'
        ]
        self.mock_ntf.return_value.__enter__.return_value.name = tmp_file
        mock_filehandle = self.mock_ntf.return_value
        mock_write = mock_filehandle.write
        with patch(
                'fsl_sub.plugins.fsl_sub_SGE.open',
                new_callable=mock_open, read_data=job_file) as m:
            m.return_value.__iter__.return_value = job_file.splitlines()
            self.mock_sprun.return_value = subprocess.CompletedProcess(
                expected_cmd, 0, stdout=qsub_out, stderr=None)
            self.assertEqual(
                jid,
                self.plugin.submit(
                    command=job_file_name,
                    job_name=job_name,
                    queue=queue,
                    array_task=True
                )
            )
            self.mock_sprun.assert_called_once_with(
                expected_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            self.mock_ntp.assert_called_once_with(
                delete=False
            )
            mock_write.assert_called_once_with(
                b'''#!/bin/sh

#$ -S /bin/sh

the_command=$(sed -n -e "${{SGE_TASK_ID}}p" {0})

exec /bin/sh -c "$the_command"
'''.format(job_file_name)
            )
            mock_osr.assert_called_once_with(tmp_file)

    @patch('fsl_sub.plugins.fsl_sub_SGE.os.remove', autospec=True)
    def test_array_submit_fails(
            self, mock_osr, mock_sprun, mock_ntf, mock_cpconf,
            mock_srbs, mock_mconf, mock_qsub,
            mock_getcwd, mock_check_pe, mock_shlex):
        job_name = 'test_job'
        queue = 'a.q'
        job_file = '''
acmd 1 2 3
acmd 4 5 6
acmd 6 7 8
'''
        job_file_name = 'll_job'
        tmp_file = 'atmpfile'
        jid = 12344
        qsub_out = 'Your job ' + jid + ' ("test_job") has been submitted'
        mock_mconf.return_value = self.mconf_dict
        expected_cmd = [
            '/usr/bin/qsub',
            '-V',
            '-binding',
            'linear:1',
            '-o', '/Users/testuser',
            '-e', '/Users/testuser',
            '-N', 'test_job',
            '-cwd', '-q', 'a.q',
            '-t', "1-4:1",
            'll_job'
        ]
        self.mock_ntf.return_value.__enter__.return_value.name = tmp_file
        mock_filehandle = self.mock_ntf.return_value
        mock_write = mock_filehandle.write
        with patch(
                'fsl_sub.plugins.fsl_sub_SGE.open',
                new_callable=mock_open, read_data=job_file) as m:
            m.return_value.__iter__.return_value = job_file.splitlines()
            self.mock_sprun.return_value = subprocess.CompletedProcess(
                expected_cmd, 1, stdout=qsub_out, stderr="Bad job")
            self.assertRaises(
                self.plugin.BadSubmission,
                self.plugin.submit,
                command=job_file_name,
                job_name=job_name,
                queue=queue,
                array_task=True
            )
            self.mock_sprun.assert_called_once_with(
                expected_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            self.mock_ntp.assert_called_once_with(
                delete=False
            )
            mock_write.assert_called_once_with(
                b'''#!/bin/sh

#$ -S /bin/sh

the_command=$(sed -n -e "${{SGE_TASK_ID}}p" {0})

exec /bin/sh -c "$the_command"
'''.format(job_file_name)
            )
            mock_osr.assert_called_once_with(tmp_file)

    @patch('fsl_sub.plugins.fsl_sub_SGE.os.remove', autospec=True)
    def test_array_submit_stride(
            self, mock_osr, mock_sprun, mock_ntf, mock_cpconf,
            mock_srbs, mock_mconf, mock_qsub,
            mock_getcwd, mock_check_pe, mock_shlex):
        job_name = 'test_job'
        queue = 'a.q'
        job_file = '''
acmd 1 2 3
acmd 4 5 6
acmd 6 7 8
'''
        job_file_name = 'll_job'
        tmp_file = 'atmpfile'
        jid = 12344
        qsub_out = 'Your job ' + jid + ' ("test_job") has been submitted'
        mock_mconf.return_value = self.mconf_dict
        expected_cmd = [
            '/usr/bin/qsub',
            '-V',
            '-binding',
            'linear:1',
            '-o', '/Users/testuser',
            '-e', '/Users/testuser',
            '-N', 'test_job',
            '-cwd', '-q', 'a.q',
            '-t', "1-8:2",
            'll_job'
        ]
        self.mock_ntf.return_value.__enter__.return_value.name = tmp_file
        mock_filehandle = self.mock_ntf.return_value
        mock_write = mock_filehandle.write
        with patch(
                'fsl_sub.plugins.fsl_sub_SGE.open',
                new_callable=mock_open, read_data=job_file) as m:
            m.return_value.__iter__.return_value = job_file.splitlines()
            self.mock_sprun.return_value = subprocess.CompletedProcess(
                expected_cmd, 0, stdout=qsub_out, stderr=None)
            self.assertEqual(
                jid,
                self.plugin.submit(
                    command=job_file_name,
                    job_name=job_name,
                    queue=queue,
                    array_task=True,
                    array_stride=2
                )
            )
            self.mock_sprun.assert_called_once_with(
                expected_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            self.mock_ntp.assert_called_once_with(
                delete=False
            )
            mock_write.assert_called_once_with(
                b'''#!/bin/sh

#$ -S /bin/sh

the_command=$(sed -n -e "${{SGE_TASK_ID}}p" {0})

exec /bin/sh -c "$the_command"
'''.format(job_file_name)
            )
            mock_osr.assert_called_once_with(tmp_file)

    @patch('fsl_sub.plugins.fsl_sub_SGE.os.remove', autospec=True)
    def test_array_limit_submit(
            self, mock_osr, mock_sprun, mock_ntf, mock_cpconf,
            mock_srbs, mock_mconf, mock_qsub,
            mock_getcwd, mock_check_pe, mock_shlex):
        job_name = 'test_job'
        queue = 'a.q'
        job_file = '''
acmd 1 2 3
acmd 4 5 6
acmd 6 7 8
'''
        job_file_name = 'll_job'
        tmp_file = 'atmpfile'
        jid = 12344
        qsub_out = 'Your job ' + jid + ' ("test_job") has been submitted'
        mock_mconf.return_value = self.mconf_dict
        expected_cmd = [
            '/usr/bin/qsub',
            '-V',
            '-binding',
            'linear:1',
            '-o', '/Users/testuser',
            '-e', '/Users/testuser',
            '-tc', "2",
            '-N', 'test_job',
            '-cwd', '-q', 'a.q',
            '-t', "1-4:1",

            'll_job'
        ]
        self.mock_ntf.return_value.__enter__.return_value.name = tmp_file
        mock_filehandle = self.mock_ntf.return_value
        mock_write = mock_filehandle.write
        with patch(
                'fsl_sub.plugins.fsl_sub_SGE.open',
                new_callable=mock_open, read_data=job_file) as m:
            m.return_value.__iter__.return_value = job_file.splitlines()
            self.mock_sprun.return_value = subprocess.CompletedProcess(
                expected_cmd, 0, stdout=qsub_out, stderr=None)
            self.assertEqual(
                jid,
                self.plugin.submit(
                    command=job_file_name,
                    job_name=job_name,
                    queue=queue,
                    array_task=True,
                    array_limit=2
                )
            )
            self.mock_sprun.assert_called_once_with(
                expected_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            self.mock_ntp.assert_called_once_with(
                delete=False
            )
            mock_write.assert_called_once_with(
                b'''#!/bin/sh

#$ -S /bin/sh

the_command=$(sed -n -e "${{SGE_TASK_ID}}p" {0})

exec /bin/sh -c "$the_command"
'''.format(job_file_name)
            )
            mock_osr.assert_called_once_with(tmp_file)

    @patch('fsl_sub.plugins.fsl_sub_SGE.os.remove', autospec=True)
    def test_array_limit_disabled_submit(
            self, mock_osr, mock_sprun, mock_ntf, mock_cpconf,
            mock_srbs, mock_mconf, mock_qsub,
            mock_getcwd, mock_check_pe, mock_shlex):
        job_name = 'test_job'
        queue = 'a.q'
        job_file = '''
acmd 1 2 3
acmd 4 5 6
acmd 6 7 8
'''
        job_file_name = 'll_job'
        tmp_file = 'atmpfile'
        jid = 12344
        qsub_out = 'Your job ' + jid + ' ("test_job") has been submitted'
        test_mconf = dict(self.mconf_dict)
        test_mconf['array_limit'] = False
        mock_mconf.return_value = test_mconf
        expected_cmd = [
            '/usr/bin/qsub',
            '-V',
            '-binding',
            'linear:1',
            '-o', '/Users/testuser',
            '-e', '/Users/testuser',
            '-N', 'test_job',
            '-cwd', '-q', 'a.q',
            '-t', "1-4:1",

            'll_job'
        ]
        self.mock_ntf.return_value.__enter__.return_value.name = tmp_file
        mock_filehandle = self.mock_ntf.return_value
        mock_write = mock_filehandle.write
        with patch(
                'fsl_sub.plugins.fsl_sub_SGE.open',
                new_callable=mock_open, read_data=job_file) as m:
            m.return_value.__iter__.return_value = job_file.splitlines()
            self.mock_sprun.return_value = subprocess.CompletedProcess(
                expected_cmd, 0, stdout=qsub_out, stderr=None)
            self.assertEqual(
                jid,
                self.plugin.submit(
                    command=job_file_name,
                    job_name=job_name,
                    queue=queue,
                    array_task=True,
                    array_limit=2
                )
            )
            self.mock_sprun.assert_called_once_with(
                expected_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            self.mock_ntp.assert_called_once_with(
                delete=False
            )
            mock_write.assert_called_once_with(
                b'''#!/bin/sh

#$ -S /bin/sh

the_command=$(sed -n -e "${{SGE_TASK_ID}}p" {0})

exec /bin/sh -c "$the_command"
'''.format(job_file_name)
            )
            mock_osr.assert_called_once_with(tmp_file)

    @patch('fsl_sub.plugins.fsl_sub_SGE.os.remove', autospec=True)
    def test_array_hold_submit(
            self, mock_osr, mock_sprun, mock_ntf, mock_cpconf,
            mock_srbs, mock_mconf, mock_qsub,
            mock_getcwd, mock_check_pe, mock_shlex):
        job_name = 'test_job'
        queue = 'a.q'
        job_file = '''
acmd 1 2 3
acmd 4 5 6
acmd 6 7 8
'''
        job_file_name = 'll_job'
        tmp_file = 'atmpfile'
        jid = 12344
        hold_jid = 12343
        qsub_out = 'Your job ' + jid + ' ("test_job") has been submitted'
        mock_mconf.return_value = self.mconf_dict
        expected_cmd = [
            '/usr/bin/qsub',
            '-V',
            '-binding',
            'linear:1',
            '-o', '/Users/testuser',
            '-e', '/Users/testuser',
            '-hold_jid_ad', hold_jid,
            '-N', 'test_job',
            '-cwd', '-q', 'a.q',
            '-t', "1-4:1",
            'll_job'
        ]
        self.mock_ntf.return_value.__enter__.return_value.name = tmp_file
        mock_filehandle = self.mock_ntf.return_value
        mock_write = mock_filehandle.write
        with patch(
                'fsl_sub.plugins.fsl_sub_SGE.open',
                new_callable=mock_open, read_data=job_file) as m:
            m.return_value.__iter__.return_value = job_file.splitlines()
            self.mock_sprun.return_value = subprocess.CompletedProcess(
                expected_cmd, 0, stdout=qsub_out, stderr=None)
            self.assertEqual(
                jid,
                self.plugin.submit(
                    command=job_file_name,
                    job_name=job_name,
                    queue=queue,
                    array_task=True,
                    array_hold=hold_jid
                )
            )
            self.mock_sprun.assert_called_once_with(
                expected_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            self.mock_ntp.assert_called_once_with(
                delete=False
            )
            mock_write.assert_called_once_with(
                b'''#!/bin/sh

#$ -S /bin/sh

the_command=$(sed -n -e "${{SGE_TASK_ID}}p" {0})

exec /bin/sh -c "$the_command"
'''.format(job_file_name)
            )
            mock_osr.assert_called_once_with(tmp_file)

    @patch('fsl_sub.plugins.fsl_sub_SGE.os.remove', autospec=True)
    def test_array_hold_disabled_submit(
            self, mock_osr, mock_sprun, mock_ntf, mock_cpconf,
            mock_srbs, mock_mconf, mock_qsub,
            mock_getcwd, mock_check_pe, mock_shlex):
        job_name = 'test_job'
        queue = 'a.q'
        job_file = '''
acmd 1 2 3
acmd 4 5 6
acmd 6 7 8
'''
        job_file_name = 'll_job'
        tmp_file = 'atmpfile'
        jid = 12344
        hold_jid = 12343
        qsub_out = 'Your job ' + jid + ' ("test_job") has been submitted'
        test_mconf = dict(self.mconf_dict)
        test_mconf['array_hold'] = False
        mock_mconf.return_value = test_mconf
        expected_cmd = [
            '/usr/bin/qsub',
            '-V',
            '-binding',
            'linear:1',
            '-o', '/Users/testuser',
            '-e', '/Users/testuser',
            '-N', 'test_job',
            '-cwd', '-q', 'a.q',
            '-t', "1-4:1",
            'll_job'
        ]
        self.mock_ntf.return_value.__enter__.return_value.name = tmp_file
        mock_filehandle = self.mock_ntf.return_value
        mock_write = mock_filehandle.write
        with patch(
                'fsl_sub.plugins.fsl_sub_SGE.open',
                new_callable=mock_open, read_data=job_file) as m:
            m.return_value.__iter__.return_value = job_file.splitlines()
            self.mock_sprun.return_value = subprocess.CompletedProcess(
                expected_cmd, 0, stdout=qsub_out, stderr=None)
            self.assertEqual(
                jid,
                self.plugin.submit(
                    command=job_file_name,
                    job_name=job_name,
                    queue=queue,
                    array_task=True,
                    array_hold=hold_jid
                )
            )
            self.mock_sprun.assert_called_once_with(
                expected_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            self.mock_ntp.assert_called_once_with(
                delete=False
            )
            mock_write.assert_called_once_with(
                b'''#!/bin/sh

#$ -S /bin/sh

the_command=$(sed -n -e "${{SGE_TASK_ID}}p" {0})

exec /bin/sh -c "$the_command"
'''.format(job_file_name)
            )
            mock_osr.assert_called_once_with(tmp_file)


if __name__ == '__main__':
    unittest.main()
