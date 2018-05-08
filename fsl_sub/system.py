import subprocess


def system_stdout(
        command, shell=False, cwd=None, timeout=None, check=True):
    result = subprocess.run(
            command,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            shell=shell, cwd=cwd, timeout=timeout,
            check=check, universal_newlines=True)

    return result.stdout.split('\n')


def system_stderr(
        command, shell=False, cwd=None, timeout=None, check=True):
    result = subprocess.run(
            command,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            shell=shell, cwd=cwd, timeout=timeout,
            check=check, universal_newlines=True)

    return result.stderr.split('\n')


def system(
        command, shell=False, cwd=None, timeout=None, check=True):
    subprocess.run(
            command,
            stderr=subprocess.PIPE,
            shell=shell, cwd=cwd, timeout=timeout,
            check=check, universal_newlines=True)
