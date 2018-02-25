#!/usr/bin/env python
import os
import subprocess
import unittest
import fsl_sub.plugins.fsl_sub_None
from unittest.mock import (patch, mock_open, call)


class TestNone(unittest.TestCase):
    @patch('fsl_sub.plugins.fsl_sub_None.os.getpid', autospec=True)
    @patch('fsl_sub.plugins.fsl_sub_None.sp.run', autospec=True)
    def test_submit(self, mock_sprun, mock_getpid):
        mock_pid = 12345
        mock_getpid.return_value = mock_pid
        logdir = "/tmp/logdir"
        jobname = "myjob"
        logfile_stdout = os.path.join(
            logdir, jobname + ".o" + str(mock_pid))
        logfile_stderr = os.path.join(
            logdir, jobname + ".e" + str(mock_pid))

        args = ['myjob', 'arg1', 'arg2', ]
        mock_sprun.return_value = subprocess.CompletedProcess(
                args, 0, "")

        mock_writer = mock_open()
        with patch('fsl_sub.plugins.fsl_sub_None.open', mock_writer):
            job_id = fsl_sub.plugins.fsl_sub_None.submit(
                command=args,
                job_name=jobname,
                queue="my.q",
                logdir=logdir)
            mock_sprun.assert_called_once_with(
                args,
                stdout=mock_writer.return_value,
                stderr=mock_writer.return_value,
                shell=True
                )

        self.assertEqual(job_id, 12345)
        expected_calls = [
            call(logfile_stdout, 'w'),
            call().__enter__(),
            call(logfile_stderr, 'w'),
            call().__enter__(),
            call().__exit__(None, None, None),
            call().__exit__(None, None, None),
        ]
        self.maxDiff = None
        self.assertListEqual(
            expected_calls,
            mock_writer.mock_calls
        )

    @patch('fsl_sub.plugins.fsl_sub_None.os.getpid')
    @patch('fsl_sub.plugins.fsl_sub_None.sp.run')
    def test_parallel_submit(self, mock_sprun, mock_getpid):
        mock_pid = 12345
        mock_getpid.return_value = mock_pid
        logdir = "/tmp/logdir"
        jobname = "myjob"
        logfile_stdout = os.path.join(
            logdir, jobname + ".o" + str(mock_pid))
        logfile_stderr = os.path.join(
            logdir, jobname + ".e" + str(mock_pid))

        ll_tests = '''mycommand arg1 arg2
mycommand2 arg3 arg4
'''

        self.maxDiff = None

        with patch(
                'fsl_sub.plugins.fsl_sub_None.open',
                new_callable=mock_open, read_data=ll_tests) as m:
            m.return_value.__iter__.return_value = ll_tests.splitlines()
            writers = [
                mock_open(), mock_open(),
                mock_open(), mock_open()]
            handlers = [
                m.return_value,
            ]
            for w in writers:
                handlers.append(w.return_value)
            m.side_effect = handlers
            command = 'anyfile'
            arraytask = True

            mock_sprun.return_value = subprocess.CompletedProcess(
                'acmd', 0, "")
            job_id = fsl_sub.plugins.fsl_sub_None.submit(
                command=command,
                job_name=jobname,
                queue='myq',
                logdir=logdir,
                array_task=arraytask,
            )
            self.assertEqual(job_id, mock_pid)
            expected_calls = [
                call(command, 'r'),
                call().__enter__(),
                call().readlines(),
                call().__exit__(None, None, None),
                call(logfile_stdout + '.1', 'w'),
                call(logfile_stderr + '.1', 'w'),
                call(logfile_stdout + '.2', 'w'),
                call(logfile_stderr + '.2', 'w'),
            ]
            self.assertEqual(
                expected_calls,
                m.mock_calls
            )
            read_calls = [
                call(command, 'r'),
                call(logfile_stdout + '.1', 'w'),
                call(logfile_stderr + '.1', 'w'),
                call(logfile_stdout + '.2', 'w'),
                call(logfile_stderr + '.2', 'w'),
            ]
            self.assertEqual(read_calls, m.call_args_list)


if __name__ == '__main__':
    unittest.main()
