from unittest.mock import AsyncMock

import pytest

from demand_link.demand_link.operational import OperationWorker


@pytest.fixture
def submission_job_holder():
    captured_jobs = []

    async def mock_submit_job(job_queue):
        while not job_queue.empty():
            job = await job_queue.get()
            if job is not None:
                captured_jobs.append(job["id"])
            job_queue.task_done()

    return mock_submit_job, captured_jobs


@pytest.mark.asyncio
async def test_jobs_are_distributed(monkeypatch, submission_job_holder):
    jobs = [{"id": f"job-{i}", "retries": 0} for i in range(6)]

    mocked_submit_job, mocked_queue_job = submission_job_holder

    fake_submission = AsyncMock()
    fake_submission.submit_job.side_effect = mocked_submit_job

    monkeypatch.setattr(
        "demand_link.demand_link.operational.Submission",
        lambda *arg, **kwrgs: fake_submission,
    )

    pool = OperationWorker(
        jobs, "http://fake-endpoint", rate_limit=10, worker_num=2
    )
    await pool.start()

    assert len(mocked_queue_job) == 6
    assert sorted(mocked_queue_job) == [f"job-{i}" for i in range(6)]
