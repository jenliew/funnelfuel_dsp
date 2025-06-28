from unittest.mock import AsyncMock

import pytest

from demand_link.demand_link.operational import WorkerManager


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


@pytest.fixture
async def mock_empty_jobs(queue):
    while not queue.empty():
        await queue.get()
        print("Got the msg. marking done")
        queue.task_done()


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

    pool = WorkerManager(
        jobs, "http://fake-endpoint", rate_limit=10, worker_num=2
    )
    await pool.start()

    assert len(mocked_queue_job) == 6
    assert sorted(mocked_queue_job) == [f"job-{i}" for i in range(6)]


@pytest.mark.asyncio
async def test_empty_job_list(monkeypatch, mock_empty_jobs):
    mock_submission = AsyncMock()

    mock_submission.submit_job.side_effect = mock_empty_jobs
    monkeypatch.setattr(
        "demand_link.demand_link.operational.Submission",
        lambda *arg, **kwrgs: mock_submission,
    )

    test_pool = WorkerManager(
        [], "http://fake-endpoint:8080", rate_limit=10, worker_num=1
    )

    await test_pool.start()
    assert mock_submission.submit_job.call_count == 1


@pytest.mark.asyncio
async def test_worker_manager_handles_empty_jobs(monkeypatch):
    fake_submission = AsyncMock()
    total_worker = 3

    fake_submission.submit_job.side_effect = mock_empty_jobs

    monkeypatch.setattr(
        "demand_link.demand_link.operational.Submission",
        lambda *a, **kw: fake_submission,
    )

    manager = WorkerManager(
        [], "http://fake-endpoint", rate_limit=1, worker_num=total_worker
    )

    await manager.start()

    assert fake_submission.submit_job.await_count == total_worker
