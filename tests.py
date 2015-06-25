from __future__ import print_function
from __future__ import unicode_literals

import sys
import pytest

import plumbum as pb

from pb_job_manager import PBJobManager

PY2 = sys.version_info[0] == 2

iteritems = (lambda d: d.iteritems()) if PY2 else (lambda d: d.items())


@pytest.fixture
def manager():
    return PBJobManager(max_procs=4, verbose=True)


def test_one_job(manager, tmpdir):
    tmp_fn = tmpdir.join("one_job.dat")
    job_cmd = pb.cmd.head["-c", "100", "/dev/urandom"] > tmp_fn.strpath
    manager.add_job(job_cmd)
    assert not tmp_fn.check()
    manager.run()
    assert tmp_fn.check()
    with tmp_fn.open('rb') as fh:
        assert len(fh.read()) == 100


def test_dependant_jobs(manager, tmpdir):
    tmp_fn = tmpdir.join("dependant_jobs.dat")
    job_id = manager.add_job([
        pb.cmd.head["-c", "100", "/dev/urandom"] > tmp_fn.strpath,
        pb.cmd.wc["-c", tmp_fn.strpath],
    ])
    manager.run()
    assert manager[job_id].stdout.startswith("100 ")


def test_job_cb_chaining(manager, tmpdir):
    tmp_fn = tmpdir.join("job_cb_chaining.dat")
    parent_results = []

    def child_cb():
        assert len(manager._done) == 1

        job_id, parent_future = next(iteritems(manager._done))
        parent_results.extend((int(char, 16) for char in "".join(
            parent_future.stdout.split()[1:-1]
        )))

        return [
            pb.cmd.echo["0" * n] >> tmp_fn.strpath
            for n in parent_results
        ]

    def parent_cb():
        return [(
            pb.cmd.head["-c", "4", "/dev/urandom"]
            | pb.cmd.hexdump
        ), child_cb]

    manager.add_job(parent_cb)
    manager.run()

    assert len(manager._done) == 9
    with tmp_fn.open() as fh:
        data = fh.read().replace("\n", "").strip()
        assert len(data) == sum(parent_results)


def test_timeout(manager, tmpdir):
    manager._job_timeout = 0.2
    job_id_1 = manager.add_job(pb.cmd.sleep[0.1])
    job_id_2 = manager.add_job(pb.cmd.sleep[0.3])

    manager.run()

    assert job_id_1 not in manager._failed
    assert job_id_2 in manager._failed


# TODO: test that would provoke IOError 24 Too many file handles, if we
# weren't cleaning up processes properly

def test_run_and_iter(manager):
    manager.add_job(pb.cmd.echo["foo"])
    manager.add_job(pb.cmd.echo["bar"])
    manager.add_job(pb.cmd.echo["baz"])

    results = [r.stdout.strip() for r in manager]
    assert len(results) == 3
    assert "foo" in results
    assert "bar" in results
    assert "baz" in results


def test_run_and_iter_over_maxprocs(manager):
    for i in range(10):
        manager.add_job(pb.cmd.echo[str(i)])

    results = [r.stdout.strip() for r in manager]
    assert len(results) == 10
    for i in range(10):
        assert str(i) in results

def test_update_during_iter(manager):
    manager.add_job(pb.cmd.echo["0"])
    for job_result in manager:
        output = int(job_result.stdout.strip())
        if output < 10:
            manager.add_job(pb.cmd.echo[str(output + 1)])

    assert output == 10
