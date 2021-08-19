#!/usr/bin/env python
import os
import shlex
import subprocess
import tempfile
import unittest
import fsl_sub.plugins.fsl_sub_plugin_shell
import fsl_sub.exceptions
from unittest.mock import (patch, ANY, )
from fsl_sub.utils import bash_cmd


class TestRequireMethods(unittest.TestCase):
    def test_available_methods(self):
        methods = dir(fsl_sub.plugins.fsl_sub_plugin_shell)
        for method in [
                'plugin_version', 'qtest', 'queue_exists',
                'submit', 'default_conf', 'job_status']:
            with self.subTest(method):
                self.assertTrue(method in methods)


class TestUtils(unittest.TestCase):
    @patch('fsl_sub.plugins.fsl_sub_plugin_shell._cores', autospec=True)
    def test__get_cores(self, mock__cores):
        mock__cores.return_value = 4
        with patch.dict('fsl_sub.plugins.fsl_sub_plugin_shell.os.environ', clear=True):
            with self.subTest("No envvar"):
                self.assertEqual(
                    fsl_sub.plugins.fsl_sub_plugin_shell._get_cores(),
                    4
                )

        with self.subTest("With envvar"):
            with patch.dict(
                    'fsl_sub.plugins.fsl_sub_plugin_shell.os.environ',
                    {'FSLSUB_PARALLEL': "2", }):
                self.assertEqual(
                    fsl_sub.plugins.fsl_sub_plugin_shell._get_cores(),
                    2
                )

        with self.subTest("With envvar=0"):
            with patch.dict(
                    'fsl_sub.plugins.fsl_sub_plugin_shell.os.environ',
                    {'FSLSUB_PARALLEL': "0", }):
                self.assertEqual(
                    fsl_sub.plugins.fsl_sub_plugin_shell._get_cores(),
                    4
                )

    def test__end_job_number(self):
        self.assertEqual(9, fsl_sub.plugins.fsl_sub_plugin_shell._end_job_number(5, 1, 2))
        self.assertEqual(10, fsl_sub.plugins.fsl_sub_plugin_shell._end_job_number(4, 1, 3))
        self.assertEqual(10, fsl_sub.plugins.fsl_sub_plugin_shell._end_job_number(3, 4, 3))

    def test__disable_parallel(self):
        with patch('fsl_sub.plugins.fsl_sub_plugin_shell.method_config', autospec=True) as mock_mc:
            with self.subTest("Is"):
                mock_mc.return_value = {'parallel_disable_matches': ['mycommand', ]}
                self.assertTrue(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('mycommand'))
                self.assertTrue(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('./mycommand'))
                self.assertTrue(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('/usr/local/bin/mycommand'))
                self.assertFalse(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('mycommand2'))
                self.assertFalse(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('amycommand'))
            with self.subTest("Is (absolute)"):
                mock_mc.return_value = {'parallel_disable_matches': ['/usr/local/bin/mycommand', ]}
                self.assertFalse(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('mycommand'))
                self.assertFalse(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('./mycommand'))
                self.assertTrue(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('/usr/local/bin/mycommand'))
                self.assertFalse(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('mycommand2'))
                self.assertFalse(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('amycommand'))
            with self.subTest("Absolute wildcards - start"):
                mock_mc.return_value = {'parallel_disable_matches': ['/usr/local/bin/*_mycommand', ]}
                self.assertFalse(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('mycommand'))
                self.assertFalse(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('./mycommand'))
                self.assertTrue(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('/usr/local/bin/bad_mycommand'))
                self.assertFalse(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('mycommand2'))
                self.assertFalse(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('amycommand'))
            with self.subTest("Absolute wildcards - end"):
                mock_mc.return_value = {'parallel_disable_matches': ['/usr/local/bin/mycommand_*', ]}
                self.assertFalse(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('mycommand'))
                self.assertFalse(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('./mycommand'))
                self.assertTrue(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('/usr/local/bin/mycommand_bad'))
                self.assertFalse(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('mycommand2'))
                self.assertFalse(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('amycommand'))
            with self.subTest("Starts"):
                mock_mc.return_value = {'parallel_disable_matches': ['mycommand_*', ]}
                self.assertFalse(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('mycommand'))
                self.assertFalse(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('./mycommand'))
                self.assertFalse(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('mycommand_'))
                self.assertTrue(
                    fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('/usr/local/bin/mycommand_special'))
                self.assertTrue(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('mycommand_special'))
                self.assertTrue(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('./mycommand_special'))
                self.assertFalse(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('amycommand_special'))
            with self.subTest("Ends"):
                mock_mc.return_value = {'parallel_disable_matches': ['*_special', ]}
                self.assertFalse(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('mycommand'))
                self.assertFalse(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('./mycommand'))
                self.assertFalse(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('_special'))
                self.assertTrue(
                    fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('/usr/local/bin/mycommand_special'))
                self.assertTrue(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('mycommand_special'))
                self.assertTrue(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('./mycommand_special'))
                self.assertFalse(fsl_sub.plugins.fsl_sub_plugin_shell._disable_parallel('mycommand_specialb'))


class TestShellReal(unittest.TestCase):
    def setUp(self):
        self.outdir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.outdir.cleanup()

    def test_simple_job(self):
        job = ["echo", "Hello"]
        jid = fsl_sub.plugins.fsl_sub_plugin_shell.submit(
            job,
            job_name='echo',
            logdir=self.outdir.name
        )
        stdout = os.path.join(self.outdir.name, 'echo.o' + str(jid))
        with open(stdout, 'r') as jobout:
            joboutput = jobout.read()

        self.assertEqual(
            joboutput,
            'Hello\n'
        )

    def test_complex_job(self):
        with self.subTest("Two commands"):
            job = ["sleep 0.1; echo 'Hello'"]
            jid = fsl_sub.plugins.fsl_sub_plugin_shell.submit(
                job,
                job_name='echo',
                logdir=self.outdir.name
            )
            stdout = os.path.join(self.outdir.name, 'echo.o' + str(jid))
            with open(stdout, 'r') as jobout:
                joboutput = jobout.read()

            self.assertEqual(
                joboutput,
                'Hello\n'
            )
        with self.subTest("Three commands"):
            job = ["sleep 0.1; echo 'Hello'; echo 'Goodbye'"]
            jid = fsl_sub.plugins.fsl_sub_plugin_shell.submit(
                job,
                job_name='echo',
                logdir=self.outdir.name
            )
            stdout = os.path.join(self.outdir.name, 'echo.o' + str(jid))
            with open(stdout, 'r') as jobout:
                joboutput = jobout.read()

            self.assertEqual(
                joboutput,
                'Hello\nGoodbye\n'
            )
        with self.subTest("Piped commands"):
            job = ["echo 'Hello' | echo 'Goodbye'"]
            jid = fsl_sub.plugins.fsl_sub_plugin_shell.submit(
                job,
                job_name='echo',
                logdir=self.outdir.name
            )
            stdout = os.path.join(self.outdir.name, 'echo.o' + str(jid))
            with open(stdout, 'r') as jobout:
                joboutput = jobout.read()

            self.assertEqual(
                joboutput,
                'Goodbye\n'
            )

    def test_set_environment(self):
        job = [bash_cmd(), '-c', "echo $FSLSUBTEST_ENVVAR", ]
        jid = fsl_sub.plugins.fsl_sub_plugin_shell.submit(
            job,
            job_name='echo',
            logdir=self.outdir.name,
            export_vars=['FSLSUBTEST_ENVVAR=1234', ]
        )
        stdout = os.path.join(self.outdir.name, 'echo.o' + str(jid))
        with open(stdout, 'r') as jobout:
            joboutput = jobout.read()

        self.assertEqual(
            joboutput,
            '1234\n'
        )

    def test_set_complexenvironment(self):
        job = [bash_cmd(), '-c', "echo $FSLSUBTEST_ENVVAR", ]
        jid = fsl_sub.plugins.fsl_sub_plugin_shell.submit(
            job,
            job_name='echo',
            logdir=self.outdir.name,
            export_vars=['FSLSUBTEST_ENVVAR="abcd=5678"', ]
        )
        stdout = os.path.join(self.outdir.name, 'echo.o' + str(jid))
        with open(stdout, 'r') as jobout:
            joboutput = jobout.read()

        self.assertEqual(
            joboutput,
            '"abcd=5678"\n'
        )


@patch('fsl_sub.plugins.fsl_sub_plugin_shell.bash_cmd', return_value="/bin/bash")
class TestShell(unittest.TestCase):
    def setUp(self):
        self.pid = os.getpid()
        self.outdir = tempfile.TemporaryDirectory()
        self.job = os.path.join(self.outdir.name, 'jobfile')
        self.errorjob = os.path.join(self.outdir.name, 'errorfile')
        self.stdout = os.path.join(self.outdir.name, 'stdout')
        self.stderr = os.path.join(self.outdir.name, 'stderr')
        self.job_id = 111
        self.p_env = {}
        self.bash = '/bin/bash'
        self.p_env['FSLSUB_JOBID_VAR'] = 'JOB_ID'
        self.p_env['FSLSUB_ARRAYTASKID_VAR'] = 'SHELL_TASK_ID'
        self.p_env['FSLSUB_ARRAYSTARTID_VAR'] = 'SHELL_TASK_FIRST'
        self.p_env['FSLSUB_ARRAYENDID_VAR'] = 'SHELL_TASK_LAST'
        self.p_env['FSLSUB_ARRAYSTEPSIZE_VAR'] = 'SHELL_TASK_STEPSIZE'
        with open(self.job, mode='w') as jobfile:
            jobfile.write(
                '''#!{0}
echo "jobid:${{!FSLSUB_JOBID_VAR}}"
echo "taskid:${{!FSLSUB_ARRAYTASKID_VAR}}"
echo "start:${{!FSLSUB_ARRAYSTARTID_VAR}}"
echo "end:${{!FSLSUB_ARRAYENDID_VAR}}"
echo "step:${{!FSLSUB_ARRAYSTEPSIZE_VAR}}"
'''.format(self.bash)
            )
        with open(self.errorjob, mode='w') as jobfile:
            jobfile.write(
                '''#!{0}
echo "jobid:${{!FSLSUB_JOBID_VAR}}" >&2
echo "taskid:${{!FSLSUB_ARRAYTASKID_VAR}}" >&2
echo "start:${{!FSLSUB_ARRAYSTARTID_VAR}}" >&2
echo "end:${{!FSLSUB_ARRAYENDID_VAR}}" >&2
echo "step:${{!FSLSUB_ARRAYSTEPSIZE_VAR}}" >&2
exit 2
'''.format(self.bash)
            )
        os.chmod(self.job, 0o755)
        os.chmod(self.errorjob, 0o755)

    def tearDown(self):
        self.outdir.cleanup()

    def test__run_job(self, mock_bash):
        job = [self.job]

        fsl_sub.plugins.fsl_sub_plugin_shell._run_job(
            job, self.job_id, self.p_env, self.stdout, self.stderr
        )

        with open(self.stdout, 'r') as jobout:
            joboutput = jobout.read()

        self.assertEqual(
            joboutput,
            '''jobid:{0}
taskid:
start:
end:
step:
'''.format(self.job_id))

    def test__run_job_stderr(self, mock_bash):
        job = [self.errorjob]

        with self.assertRaises(fsl_sub.exceptions.BadSubmission) as bs:
            fsl_sub.plugins.fsl_sub_plugin_shell._run_job(
                job, self.job_id, self.p_env, self.stdout, self.stderr
            )

        joboutput = str(bs.exception)

        self.assertEqual(
            joboutput,
            '''jobid:{0}
taskid:
start:
end:
step:
'''.format(self.job_id)
        )

    @patch(
        'fsl_sub.plugins.fsl_sub_plugin_shell._get_cores',
        autospec=True)
    def test__run_parallel_all(self, mock_gc, mock_bash):
        jobs = [self.job, self.job, self.job, ]

        mock_gc.return_value = 4
        fsl_sub.plugins.fsl_sub_plugin_shell._run_parallel(
            jobs, self.job_id, self.p_env, self.stdout, self.stderr
        )
        job_list = []
        for subjob in (1, 2, 3):
            jobout = '.'.join((self.stdout, str(subjob)))
            joberr = '.'.join((self.stderr, str(subjob)))
            child_env = dict(self.p_env)
            child_env['JOB_ID'] = self.job_id
            child_env['SHELL_TASK_ID'] = subjob
            child_env['SHELL_TASK_FIRST'] = 1
            child_env['SHELL_TASK_LAST'] = 3
            child_env['SHELL_TASK_STEPSIZE'] = 1
            with open(jobout, 'r') as jout:
                joboutput = jout.read()
            with open(joberr, 'r') as jerr:
                joberror = jerr.read()
            job_list.append([self.job, subjob, child_env, jobout, joberr])
            self.assertEqual(
                joboutput,
                '''jobid:{0}
taskid:{1}
start:{2}
end:{3}
step:{4}
'''.format(self.job_id, subjob, 1, 3, 1))
            self.assertEqual(joberror, '')

    @patch(
        'fsl_sub.plugins.fsl_sub_plugin_shell._get_cores',
        autospec=True)
    def test__run_parallel_cpulimited(self, mock_gc, mock_bash):
        mock_gc.return_value = 2

        jobs = [self.job, self.job, self.job, ]

        fsl_sub.plugins.fsl_sub_plugin_shell._run_parallel(
            jobs, self.job_id, self.p_env, self.stdout, self.stderr
        )

        job_list = []
        for subjob in (1, 2, 3):
            jobout = '.'.join((self.stdout, str(subjob)))
            joberr = '.'.join((self.stderr, str(subjob)))
            child_env = dict(self.p_env)
            child_env['JOB_ID'] = self.job_id
            child_env['SHELL_TASK_ID'] = str(subjob)
            child_env['SHELL_TASK_FIRST'] = str(1)
            child_env['SHELL_TASK_LAST'] = str(3)
            child_env['SHELL_TASK_STEPSIZE'] = str(1)
            with open(jobout, 'r') as jout:
                joboutput = jout.read()
            with open(joberr, 'r') as jerr:
                joberror = jerr.read()
            job_list.append([self.job, subjob, child_env, jobout, joberr])
            self.assertEqual(
                joboutput,
                '''jobid:{0}
taskid:{1}
start:{2}
end:{3}
step:{4}
'''.format(self.job_id, subjob, 1, 3, 1))
            self.assertEqual(joberror, '')

    @patch(
        'fsl_sub.plugins.fsl_sub_plugin_shell._get_cores',
        autospec=True)
    def test__run_parallel_threadlimited(self, mock_gc, mock_bash):
        mock_gc.return_value = 4
        jobs = [self.job, self.job, self.job, ]

        fsl_sub.plugins.fsl_sub_plugin_shell._run_parallel(
            jobs, self.job_id, self.p_env, self.stdout, self.stderr,
            parallel_limit=2
        )

        job_list = []
        for subjob in (1, 2, 3):
            jobout = '.'.join((self.stdout, str(subjob)))
            joberr = '.'.join((self.stderr, str(subjob)))
            child_env = dict(self.p_env)
            child_env['JOB_ID'] = self.job_id
            child_env['SHELL_TASK_ID'] = subjob
            child_env['SHELL_TASK_FIRST'] = 1
            child_env['SHELL_TASK_LAST'] = 3
            child_env['SHELL_TASK_STEPSIZE'] = 1
            job_list.append([self.job, subjob, child_env, jobout, joberr])
            with open(jobout, 'r') as jout:
                joboutput = jout.read()
            with open(joberr, 'r') as jerr:
                joberror = jerr.read()
            self.assertEqual(
                joboutput,
                '''jobid:{0}
taskid:{1}
start:{2}
end:{3}
step:{4}
'''.format(self.job_id, subjob, 1, 3, 1))
            self.assertEqual(joberror, '')

    @patch(
        'fsl_sub.plugins.fsl_sub_plugin_shell._get_cores',
        autospec=True)
    def test__run_parallel_spec(self, mock_gc, mock_bash):
        mock_gc.return_value = 4
        jobs = [self.job, self.job, self.job, ]

        fsl_sub.plugins.fsl_sub_plugin_shell._run_parallel(
            jobs, self.job_id, self.p_env, self.stdout, self.stderr,
            parallel_limit=2
        )

        job_list = []
        for subjob in (1, 2, 3):
            jobout = '.'.join((self.stdout, str(subjob)))
            joberr = '.'.join((self.stderr, str(subjob)))
            child_env = dict(self.p_env)
            child_env['JOB_ID'] = self.job_id
            child_env['SHELL_TASK_ID'] = subjob
            child_env['SHELL_TASK_FIRST'] = 1
            child_env['SHELL_TASK_LAST'] = 3
            child_env['SHELL_TASK_STEPSIZE'] = 1
            job_list.append([self.job, subjob, child_env, jobout, joberr])
            with open(jobout, 'r') as jout:
                joboutput = jout.read()
            with open(joberr, 'r') as jerr:
                joberror = jerr.read()
            self.assertEqual(
                joboutput,
                '''jobid:{0}
taskid:{1}
start:{2}
end:{3}
step:{4}
'''.format(self.job_id, subjob, 1, 3, 1))
            self.assertEqual(joberror, '')

    @patch('fsl_sub.plugins.fsl_sub_plugin_shell.os.getpid', autospec=True)
    @patch('fsl_sub.plugins.fsl_sub_plugin_shell._run_job', autospec=True)
    def test_submit(self, mock__run_job, mock_getpid, mock_bash):
        mock_pid = 12345
        mock_getpid.return_value = mock_pid
        logdir = "/tmp/logdir"
        jobname = "myjob"
        logfile_stdout = os.path.join(
            logdir, jobname + ".o" + str(mock_pid))
        logfile_stderr = os.path.join(
            logdir, jobname + ".e" + str(mock_pid))

        args = ['myjob', 'arg1', 'arg2', ]

        test_environ = {'AVAR': 'AVAL', }
        result_environ = dict(test_environ)
        result_environ['FSLSUB_JOBID_VAR'] = 'JOB_ID'
        result_environ['FSLSUB_ARRAYTASKID_VAR'] = 'SHELL_TASK_ID'
        result_environ['FSLSUB_ARRAYSTARTID_VAR'] = 'SHELL_TASK_FIRST'
        result_environ['FSLSUB_ARRAYENDID_VAR'] = 'SHELL_TASK_LAST'
        result_environ['FSLSUB_ARRAYSTEPSIZE_VAR'] = 'SHELL_TASK_STEPSIZE'
        result_environ['FSLSUB_ARRAYCOUNT_VAR'] = 'SHELL_ARRAYCOUNT'
        result_environ['FSLSUB_PARALLEL'] = '1'
        # result_environ['JOB_ID'] = str(mock_pid) - mocked so doesn't get set
        with patch.dict(
                'fsl_sub.plugins.fsl_sub_plugin_shell.os.environ',
                test_environ,
                clear=True):
            fsl_sub.plugins.fsl_sub_plugin_shell.submit(
                command=args,
                job_name=jobname,
                queue="my.q",
                logdir=logdir)
            mock__run_job.assert_called_once_with(
                args,
                mock_pid,
                result_environ,
                logfile_stdout,
                logfile_stderr
            )

    @patch('fsl_sub.plugins.fsl_sub_plugin_shell.os.getpid', autospec=True)
    @patch('fsl_sub.plugins.fsl_sub_plugin_shell.sp.run', autospec=True)
    def test_quoted_arg_submit(self, mock_sp_run, mock_getpid, mock_bash):
        mock_pid = 12345
        mock_getpid.return_value = mock_pid
        with tempfile.TemporaryDirectory() as tempdir:
            logdir = tempdir
            jobname = "myjob"

            args = ['myjob', '-arg1', '-arg2', '-arg3', "fprintf(1,'Hello World\n');"]
            runner = [mock_bash.return_value, '-c', ]
            mock_sp_run.return_value = subprocess.CompletedProcess(runner + args, 0, '', '')

            test_environ = {'AVAR': 'AVAL', }
            result_environ = dict(test_environ)
            result_environ['FSLSUB_JOBID_VAR'] = 'JOB_ID'
            result_environ['FSLSUB_ARRAYTASKID_VAR'] = 'SHELL_TASK_ID'
            result_environ['FSLSUB_ARRAYSTARTID_VAR'] = 'SHELL_TASK_FIRST'
            result_environ['FSLSUB_ARRAYENDID_VAR'] = 'SHELL_TASK_LAST'
            result_environ['FSLSUB_ARRAYSTEPSIZE_VAR'] = 'SHELL_TASK_STEPSIZE'
            result_environ['FSLSUB_ARRAYCOUNT_VAR'] = 'SHELL_ARRAYCOUNT'
            result_environ['FSLSUB_PARALLEL'] = '1'
            result_environ['JOB_ID'] = str(mock_pid)
            with patch.dict(
                    'fsl_sub.plugins.fsl_sub_plugin_shell.os.environ',
                    test_environ,
                    clear=True):
                fsl_sub.plugins.fsl_sub_plugin_shell.submit(
                    command=args,
                    job_name=jobname,
                    queue="my.q",
                    logdir=logdir)
                mock_sp_run.assert_called_once_with(
                    runner + [" ".join(args)],
                    env=result_environ,
                    stdout=ANY,
                    stderr=ANY,
                    universal_newlines=True
                )

    @patch('fsl_sub.plugins.fsl_sub_plugin_shell.os.getpid')
    @patch('fsl_sub.plugins.fsl_sub_plugin_shell._run_parallel')
    def test_parallel_submit(self, mock__run_parallel, mock_getpid, mock_bash):
        mock_pid = 12345
        mock_getpid.return_value = mock_pid
        logdir = "/tmp/logdir"
        jobname = "myjob"
        logfile_stdout = os.path.join(
            logdir, jobname + ".o" + str(mock_pid))
        logfile_stderr = os.path.join(
            logdir, jobname + ".e" + str(mock_pid))
        ll_tests = ['mycommand arg1 arg2', 'mycommand2 arg3 arg4', ]

        with tempfile.TemporaryDirectory() as tempdir:
            job_file = os.path.join(tempdir, 'myjob')
            with open(job_file, mode='w') as jf:
                jf.writelines([a + '\n' for a in ll_tests])

            test_environ = {'AVAR': 'AVAL', }
            result_environ = dict(test_environ)
            result_environ['FSLSUB_JOBID_VAR'] = 'JOB_ID'
            result_environ['FSLSUB_ARRAYTASKID_VAR'] = 'SHELL_TASK_ID'
            result_environ['FSLSUB_ARRAYSTARTID_VAR'] = 'SHELL_TASK_FIRST'
            result_environ['FSLSUB_ARRAYENDID_VAR'] = 'SHELL_TASK_LAST'
            result_environ['FSLSUB_ARRAYSTEPSIZE_VAR'] = 'SHELL_TASK_STEPSIZE'
            result_environ['FSLSUB_ARRAYCOUNT_VAR'] = 'SHELL_ARRAYCOUNT'
            result_environ['FSLSUB_PARALLEL'] = '1'
            # result_environ['JOB_ID'] = str(mock_pid) - mocked so doesn't get set
            with patch.dict(
                    'fsl_sub.plugins.fsl_sub_plugin_shell.os.environ',
                    test_environ,
                    clear=True):
                fsl_sub.plugins.fsl_sub_plugin_shell.submit(
                    command=[job_file],
                    job_name=jobname,
                    queue="my.q",
                    array_task=True,
                    logdir=logdir)
                mock__run_parallel.assert_called_once_with(
                    [shlex.split(a) for a in ll_tests],
                    mock_pid,
                    result_environ,
                    logfile_stdout,
                    logfile_stderr
                )

    @patch('fsl_sub.plugins.fsl_sub_plugin_shell.os.getpid')
    @patch('fsl_sub.plugins.fsl_sub_plugin_shell._run_parallel')
    def test_parallel_submit_bash_singlelines(self, mock__run_parallel, mock_getpid, mock_bash):
        mock_pid = 12345
        mock_getpid.return_value = mock_pid
        logdir = "/tmp/logdir"
        jobname = "myjob"
        logfile_stdout = os.path.join(
            logdir, jobname + ".o" + str(mock_pid))
        logfile_stderr = os.path.join(
            logdir, jobname + ".e" + str(mock_pid))
        ll_tests = ['MYENVVAR=1234; sleep 1; mycommand arg1 arg2', 'MYENVVAR=5678; sleep 3; mycommand2 arg3 arg4', ]
        ll_out = [[mock_bash.return_value, '-c', a] for a in ll_tests]
        with tempfile.TemporaryDirectory() as tempdir:
            job_file = os.path.join(tempdir, 'myjob')
            with open(job_file, mode='w') as jf:
                jf.writelines([a + '\n' for a in ll_tests])

            test_environ = {'AVAR': 'AVAL', }
            result_environ = dict(test_environ)
            result_environ['FSLSUB_JOBID_VAR'] = 'JOB_ID'
            result_environ['FSLSUB_ARRAYTASKID_VAR'] = 'SHELL_TASK_ID'
            result_environ['FSLSUB_ARRAYSTARTID_VAR'] = 'SHELL_TASK_FIRST'
            result_environ['FSLSUB_ARRAYENDID_VAR'] = 'SHELL_TASK_LAST'
            result_environ['FSLSUB_ARRAYSTEPSIZE_VAR'] = 'SHELL_TASK_STEPSIZE'
            result_environ['FSLSUB_ARRAYCOUNT_VAR'] = 'SHELL_ARRAYCOUNT'
            result_environ['FSLSUB_PARALLEL'] = '1'
            # result_environ['JOB_ID'] = str(mock_pid) - mocked so doesn't get set
            with patch.dict(
                    'fsl_sub.plugins.fsl_sub_plugin_shell.os.environ',
                    test_environ,
                    clear=True):
                fsl_sub.plugins.fsl_sub_plugin_shell.submit(
                    command=[job_file],
                    job_name=jobname,
                    queue="my.q",
                    array_task=True,
                    logdir=logdir)
                mock__run_parallel.assert_called_once_with(
                    ll_out,
                    mock_pid,
                    result_environ,
                    logfile_stdout,
                    logfile_stderr
                )

    @patch('fsl_sub.plugins.fsl_sub_plugin_shell.os.getpid')
    @patch('fsl_sub.plugins.fsl_sub_plugin_shell._run_parallel')
    @patch(
        'fsl_sub.plugins.fsl_sub_plugin_shell.method_config',
        autospec=True,
        return_value={'parallel_disable_matches': '*_gpu'})
    def test_parallel_submit_jname_disable(self, mock_mconf, mock__run_parallel, mock_getpid, mock_bash):
        mock_pid = 12345
        mock_getpid.return_value = mock_pid
        logdir = "/tmp/logdir"
        jobname = "myjob"
        logfile_stdout = os.path.join(
            logdir, jobname + ".o" + str(mock_pid))
        logfile_stderr = os.path.join(
            logdir, jobname + ".e" + str(mock_pid))
        ll_tests = ['mycommand_gpu arg1 arg2',
                    'mycommand2 arg3 arg4',
                    '"mycommand3" arg5 arg6',
                    '"/spacy dir/mycommand4" arg5 arg6',
                    '/spacy\\ dir/mycommand4 arg5 arg6']

        with tempfile.TemporaryDirectory() as tempdir:
            job_file = os.path.join(tempdir, 'myjob')
            with open(job_file, mode='w') as jf:
                jf.writelines([a + '\n' for a in ll_tests])

            test_environ = {'AVAR': 'AVAL', }
            result_environ = dict(test_environ)
            result_environ['FSLSUB_JOBID_VAR'] = 'JOB_ID'
            result_environ['FSLSUB_ARRAYTASKID_VAR'] = 'SHELL_TASK_ID'
            result_environ['FSLSUB_ARRAYSTARTID_VAR'] = 'SHELL_TASK_FIRST'
            result_environ['FSLSUB_ARRAYENDID_VAR'] = 'SHELL_TASK_LAST'
            result_environ['FSLSUB_ARRAYSTEPSIZE_VAR'] = 'SHELL_TASK_STEPSIZE'
            result_environ['FSLSUB_ARRAYCOUNT_VAR'] = 'SHELL_ARRAYCOUNT'
            result_environ['FSLSUB_PARALLEL'] = '1'
            # result_environ['JOB_ID'] = str(mock_pid) - mocked so doesn't get set
            with patch.dict(
                    'fsl_sub.plugins.fsl_sub_plugin_shell.os.environ',
                    test_environ,
                    clear=True):
                fsl_sub.plugins.fsl_sub_plugin_shell.submit(
                    command=[job_file],
                    job_name=jobname,
                    queue="my.q",
                    array_task=True,
                    logdir=logdir)
                mock__run_parallel.assert_called_once_with(
                    [shlex.split(a) for a in ll_tests],
                    mock_pid,
                    result_environ,
                    logfile_stdout,
                    logfile_stderr,
                    parallel_limit=1
                )

    @patch('fsl_sub.plugins.fsl_sub_plugin_shell.os.getpid')
    @patch('fsl_sub.plugins.fsl_sub_plugin_shell._run_parallel')
    def test_parallel_submit_spec(self, mock__run_parallel, mock_getpid, mock_bash):
        mock_pid = 12345
        mock_getpid.return_value = mock_pid
        logdir = "/tmp/logdir"
        jobname = "myjob"
        spec = "4-8:4"
        njobs = 2
        logfile_stdout = os.path.join(
            logdir, jobname + ".o" + str(mock_pid))
        logfile_stderr = os.path.join(
            logdir, jobname + ".e" + str(mock_pid))

        command = ['acmd', ]
        arraytask = True

        test_environ = {'AVAR': 'AVAL', }
        result_environ = dict(test_environ)
        result_environ['FSLSUB_JOBID_VAR'] = 'JOB_ID'
        result_environ['FSLSUB_ARRAYTASKID_VAR'] = 'SHELL_TASK_ID'
        result_environ['FSLSUB_ARRAYSTARTID_VAR'] = 'SHELL_TASK_FIRST'
        result_environ['FSLSUB_ARRAYENDID_VAR'] = 'SHELL_TASK_LAST'
        result_environ['FSLSUB_ARRAYSTEPSIZE_VAR'] = 'SHELL_TASK_STEPSIZE'
        result_environ['FSLSUB_ARRAYCOUNT_VAR'] = 'SHELL_ARRAYCOUNT'
        result_environ['FSLSUB_PARALLEL'] = '1'
        # result_environ['JOB_ID'] = str(mock_pid) - mocked so doesn't get set
        with patch.dict(
                'fsl_sub.plugins.fsl_sub_plugin_shell.os.environ',
                test_environ,
                clear=True):
            fsl_sub.plugins.fsl_sub_plugin_shell.submit(
                command=command,
                job_name=jobname,
                queue="my.q",
                array_task=arraytask,
                array_specifier=spec,
                logdir=logdir)
            mock__run_parallel.assert_called_once_with(
                njobs * [command],
                mock_pid,
                result_environ,
                logfile_stdout,
                logfile_stderr,
                array_start=4,
                array_end=8,
                array_stride=4
            )

    @patch('fsl_sub.plugins.fsl_sub_plugin_shell.os.getpid')
    @patch('fsl_sub.plugins.fsl_sub_plugin_shell._run_parallel')
    @patch(
        'fsl_sub.plugins.fsl_sub_plugin_shell.method_config',
        autospec=True,
        return_value={'parallel_disable_matches': '*_gpu'})
    def test_parallel_submit_spec_jname_disable(self, mock_mconf, mock__run_parallel, mock_getpid, mock_bash):
        mock_pid = 12345
        mock_getpid.return_value = mock_pid
        logdir = "/tmp/logdir"
        jobname = "myjob"
        spec = "4-8:4"
        njobs = 2
        logfile_stdout = os.path.join(
            logdir, jobname + ".o" + str(mock_pid))
        logfile_stderr = os.path.join(
            logdir, jobname + ".e" + str(mock_pid))

        command = ['acmd_gpu', ]
        arraytask = True

        test_environ = {'AVAR': 'AVAL', }
        result_environ = dict(test_environ)
        result_environ['FSLSUB_JOBID_VAR'] = 'JOB_ID'
        result_environ['FSLSUB_ARRAYTASKID_VAR'] = 'SHELL_TASK_ID'
        result_environ['FSLSUB_ARRAYSTARTID_VAR'] = 'SHELL_TASK_FIRST'
        result_environ['FSLSUB_ARRAYENDID_VAR'] = 'SHELL_TASK_LAST'
        result_environ['FSLSUB_ARRAYSTEPSIZE_VAR'] = 'SHELL_TASK_STEPSIZE'
        result_environ['FSLSUB_ARRAYCOUNT_VAR'] = 'SHELL_ARRAYCOUNT'
        result_environ['FSLSUB_PARALLEL'] = '1'
        # result_environ['JOB_ID'] = str(mock_pid) - mocked so doesn't get set
        with patch.dict(
                'fsl_sub.plugins.fsl_sub_plugin_shell.os.environ',
                test_environ,
                clear=True):
            fsl_sub.plugins.fsl_sub_plugin_shell.submit(
                command=command,
                job_name=jobname,
                queue="my.q",
                array_task=arraytask,
                array_specifier=spec,
                logdir=logdir)
            mock__run_parallel.assert_called_once_with(
                njobs * [command],
                mock_pid,
                result_environ,
                logfile_stdout,
                logfile_stderr,
                array_start=4,
                array_end=8,
                array_stride=4,
                parallel_limit=1
            )


if __name__ == '__main__':
    unittest.main()
