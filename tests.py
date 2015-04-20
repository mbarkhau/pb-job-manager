from __future__ import print_function
from __future__ import unicode_literals

import sys
import time
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
