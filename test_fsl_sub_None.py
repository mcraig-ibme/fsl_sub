#!/usr/bin/env python
import unittest
import fsl_sub_None
from unittest.mock import (patch, mock_open, call)


class TestNone(unittest.TestCase):
    @patch('fsl_sub_None.os.getpid')
    @patch('fsl_sub_None.sp.run')
    def test_submit(self, mock_sprun, mock_getpid):
        mock_getpid.return_value = 12345
        options = {}
        options['logdir'] = "/tmp/logdir"
        options['jobname'] = "myjob"
        logfile_stdout = options['logdir'] + ".o12345"
        logfile_stderr = options['logdir'] + ".e12345"

        options['args'] = ['myjob', 'arg1', 'arg2', ]
        mock_writer = mock_open()
        with patch('fsl_sub_None.open', mock_writer):
            mock_sprun.return_value = 0
            job_id = fsl_sub_None.submit({}, options)
        self.assertEqual(job_id, 12345)
        expected_calls = [
            call(logfile_stdout, 'w'),
            call().__enter__(),
            call(logfile_stderr, 'w'),
            call().__enter__(),
            call().__exit__(None, None, None),
            call().__exit__(None, None, None),
        ]
        self.assertEqual(
            expected_calls,
            mock_writer.mock_calls
        )
        mock_sprun.assert_called_once_with(
            options['args'],
            stdout=logfile_stdout,
            stderr=logfile_stderr,
            shell=True
        )

    @patch('fsl_sub_None.os.getpid')
    @patch('fsl_sub_None.sp.run')
    def test_parallel_submit(self, mock_sprun, mock_getpid):
        mock_getpid.return_value = 12345
        options = {}
        options['logdir'] = "/tmp/logdir"
        options['jobname'] = "myjob"
        logfile_stdout = options['logdir'] + ".o12345"
        logfile_stderr = options['logdir'] + ".e12345"

        ll_tests = '''mycommand arg1 arg2
mycommand2 arg3 arg4
'''

        with patch(
                '__main__.open',
                unittest.mock.mock_open(read_data=ll_tests)) as m:
            m.return_value.__iter__.return_value = ll_tests.splitlines()
            options['paralleltask'] = open('anyfile', 'r')
            options['paralleltask'].close()

            mock_writer = unittest.mock.mock_open()
            with patch('fsl_sub_None.open', mock_writer):
                mock_sprun.return_value = 0
                job_id = fsl_sub_None.submit({}, options)
            self.assertEqual(job_id, 12345)
            expected_calls = [
                call(logfile_stdout + '.1', 'w'),
                call().__enter__(),
                call(logfile_stderr + '.1', 'w'),
                call().__enter__(),
                call().__exit__(None, None, None),
                call().__exit__(None, None, None),
                call(logfile_stdout + '.2', 'w'),
                call().__enter__(),
                call(logfile_stderr + '.2', 'w'),
                call().__enter__(),
                call().__exit__(None, None, None),
                call().__exit__(None, None, None),
            ]
            self.assertEqual(
                expected_calls,
                mock_writer.mock_calls
            )
            read_calls = [
                call(options['paralleltask'].name, 'r')
            ]
            self.assertEqual(read_calls, m.call_args_list)


if __name__ == '__main__':
    unittest.main()
