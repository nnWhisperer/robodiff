import uuid
import datetime
import threading
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
from collections import deque, defaultdict
import json
import os
import gzip
from concurrent.futures import ThreadPoolExecutor


# Define the MagicNumbers constants
class MagicNumbers:
    ERROR_COUNT_UNTIL_FAILURE = 10
    TIMEOUT_TIME_SPAN = datetime.timedelta(minutes=10)
    DEFAULT_RETRY_DELAY_SECONDS = 10

# Enum for ResultsStatuses
class ResultsStatuses(Enum):
    PENDING = "PENDING"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"

# Define the record-like classes
class ExportFolderName:
    def __init__(self, folder_name: str, compression: bool):
        self.folder_name = folder_name
        self.compression = compression

class JobSubmissionRecord:
    def __init__(self, inner_simulations: List[Any], job_details: Optional[Any] = None):
        self.inner_simulations = inner_simulations
        self.job_details = job_details

class SimulationRecord:
    def __init__(self, job_guid: uuid.UUID, sim_idx: int, retry_count: int):
        self.job_guid = job_guid
        self.sim_idx = sim_idx
        self.retry_count = retry_count

class DispatchedWorkRecord:
    def __init__(self, work_guid: uuid.UUID, node_guid: uuid.UUID, sim_record: SimulationRecord):
        self.work_guid = work_guid
        self.node_guid = node_guid
        self.sim_record = sim_record

class SimulationDetails:
    def __init__(self, work_guid: uuid.UUID, inner_simulation: Any, retry_delay: int):
        self.work_guid = work_guid
        self.inner_simulation = inner_simulation
        self.retry_delay = retry_delay

import uuid
import datetime
import threading
from collections import deque, defaultdict
from typing import Dict, List, Optional, Tuple

class NodeWorkInfo:
    def __init__(self, work_guid):
        self.work_guid = work_guid
        self.start_time = datetime.datetime.now()
        self.finish_time = None
        self.check_in_count = 0
        self.status = ResultsStatuses.PENDING

    def update_work(self):
        self.check_in_count += 1

    def finish_work(self):
        self.finish_time = datetime.datetime.now()
        self.status = ResultsStatuses.COMPLETE

    def error_work(self):
        self.status = ResultsStatuses.FAILED


class NodeInfo:
    def __init__(self, node_guid, error_count, details):
        self.node_guid = node_guid
        self._error_cnt = error_count
        self.details = details
        self.last_contact_time = datetime.datetime.now()
        self.node_is_shutdown = False
        self.work_log = []
        self.current_work = None

    def start_new_work(self, work_guid):
        self.current_work = NodeWorkInfo(work_guid)
        self.work_log.append(self.current_work)

    def update_work(self):
        if self.current_work:
            self.current_work.update_work()
        self.update_last_contact_time()

    def error_work(self):
        if self.current_work:
            self.current_work.error_work()
        self.update_last_contact_time()
        self._error_cnt += 1
        return self._error_cnt

    def finish_work(self):
        if self.current_work:
            self.current_work.finish_work()
        self.update_last_contact_time()

    def shutdown_node(self):
        self.update_last_contact_time()
        self.node_is_shutdown = True

    def is_node_active(self):
        return not self.node_is_shutdown and self.last_contact_time + MagicNumbers.TimeoutTimeSpan >= datetime.datetime.now()

    def update_last_contact_time(self):
        self.last_contact_time = datetime.datetime.now()


class SimulationResults:
    def __init__(self):
        self.results_list = []
        self.results_status = ResultsStatuses.PENDING
        self.last_contact = datetime.datetime.now()
        self.error_details = None
        self._lock = threading.Lock()

    @property
    def results_status(self):
        with self._lock:
            return self._results_status

    @results_status.setter
    def results_status(self, value):
        with self._lock:
            self._results_status = value
            self.last_contact = datetime.datetime.now()

    @property
    def results_status_str(self):
        return self.results_status.name

    @property
    def last_contact(self):
        with self._lock:
            return self._last_contact

    @property
    def error_details(self):
        with self._lock:
            return self._error_details

    @error_details.setter
    def error_details(self, value):
        with self._lock:
            self._error_details = value

    def check_and_mark_timeout(self, timeout_time_span):
        with self._lock:
            if self.results_status == ResultsStatuses.PENDING and self.last_contact + timeout_time_span < datetime.datetime.now():
                self.results_status = ResultsStatuses.FAILED
                return True
            return False

    def append_results(self, partial_results):
        with self._lock:
            self.results_list.append(partial_results)
            self.last_contact = datetime.datetime.now()

    def get_results(self):
        with self._lock:
            return self.results_list[:]

    def export(self, work_guid):
        return ExportSimulationResultsRecord(work_guid, self.last_contact, self.results_status, self.get_results(), self.error_details)


class Simulation:
    def __init__(self, inner_sim):
        self._inner_sim = inner_sim
        self.sim_results = {}
        self._lock = threading.Lock()

    @property
    def inner_sim(self):
        with self._lock:
            return self._inner_sim

    def fetch_simulation_results(self):
        with self._lock:
            return self.sim_results.items()

    def init_sim_results(self, work_guid):
        with self._lock:
            self.sim_results[work_guid] = SimulationResults()
            return self._inner_sim

    def get_simulation_results(self, work_guid):
        with self._lock:
            return self.sim_results.get(work_guid)

    def is_completed(self):
        return any(result.results_status == ResultsStatuses.COMPLETE for _, result in self.fetch_simulation_results())

    def get_completed_results(self):
        for _, result in self.fetch_simulation_results():
            if result.results_status == ResultsStatuses.COMPLETE:
                return ExportSimulationRecord(self.inner_sim, result.export(_))
        return None

    def get_all_results(self):
        results_records = [result.export(work_guid) for work_guid, result in self.fetch_simulation_results()]
        return ExportSimulationRecordDetailed(self.inner_sim, results_records)


class Job:
    def __init__(self, inner_simulations, job_details=None):
        self.job_guid = uuid.uuid4()
        self._job_details = job_details
        self.simulations = [Simulation(x) for x in inner_simulations]
        self.curr_sim_idx = 0
        self._lock = threading.Lock()

    @property
    def job_guid(self):
        with self._lock:
            return self._job_guid

    @property
    def curr_sim_idx(self):
        with self._lock:
            return self._curr_sim_idx

    @property
    def job_details(self):
        with self._lock:
            return self._job_details

    def get_simulation(self, sim_idx):
        if sim_idx < len(self.simulations):
            return self.simulations[sim_idx]
        return None

    def get_inner_sim(self, sim_idx):
        sim = self.get_simulation(sim_idx)
        return sim.inner_sim if sim else None

    def get_simulation_results(self, sim_idx, work_guid):
        sim = self.get_simulation(sim_idx)
        return sim.get_simulation_results(work_guid) if sim else None

    def get_next_simulation_record(self):
        with self._lock:
            if self._curr_sim_idx < len(self.simulations):
                sim_record = SimulationRecord(self.job_guid, self._curr_sim_idx, 0)
                self._curr_sim_idx += 1
                return sim_record
        return None

    def export_job_summary(self):
        total_sim_count = len(self.simulations)
        completed_sim_count = sum(1 for sim in self.simulations if sim.is_completed())
        return ExportJobRecord(self.job_guid, self.job_details, self.curr_sim_idx, completed_sim_count, total_sim_count)

    def export_completed_simulations_folder(self, folder, compression=True):
        """
        Export completed simulations to a specified folder with optional compression.
        """
        job_folder = os.path.join(folder, str(self.job_guid))
        os.makedirs(job_folder, exist_ok=True)

        file_name = os.path.join(job_folder, 'completedSimulations.json')
        if compression:
            file_name += '.gz'
            with gzip.open(file_name, 'wt', encoding='utf-8') as file_out:
                self.export_completed_simulations_file(file_out)
        else:
            with open(file_name, 'w', encoding='utf-8') as file_out:
                self.export_completed_simulations_file(file_out)

    def export_all_simulations_folder(self, folder, compression=True):
        """
        Export all simulations to a specified folder with optional compression.
        """
        job_folder = os.path.join(folder, str(self.job_guid))
        os.makedirs(job_folder, exist_ok=True)

        file_name = os.path.join(job_folder, 'allSimulations.json')
        if compression:
            file_name += '.gz'
            with gzip.open(file_name, 'wt', encoding='utf-8') as file_out:
                self.export_all_simulations_file(file_out)
        else:
            with open(file_name, 'w', encoding='utf-8') as file_out:
                self.export_all_simulations_file(file_out)

    def export_completed_simulations_file(self, file_out):
        """
        Write completed simulations to a file.
        """
        for simulation in self.simulations:
            record = simulation.get_completed_results()
            if record is not None:
                json.dump(record, file_out)
                file_out.write('\n')

    def export_all_simulations_file(self, file_out):
        """
        Write all simulations to a file.
        """
        for simulation in self.simulations:
            record = simulation.get_all_results()
            if record is not None:
                json.dump(record, file_out)
                file_out.write('\n')

import threading
import uuid
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

class JobRepository:
    def __init__(self):
        self.jobs_dict_lock = threading.Lock()
        self.jobs_dict = {}

        self.primary_queue_lock = threading.Lock()
        self.primary_queue = []

        self.current_job_idx = 0
        self.total_simulation_count = 0
        self.dispatched_simulation_count = 0
        self.total_simulation_error_count = 0
        self.failed_simulation_count = 0
        self.total_results_received_count = 0
        self.total_finished_simulation_count = 0

        self.outstanding_work_dict_lock = threading.Lock()
        self.outstanding_work_dict = {}

        self.retry_queue = Queue()

        self.node_info_dict_lock = threading.Lock()
        self.node_info_dict = {}

    def shutdown_node(self, node_guid):
        with self.node_info_dict_lock:
            node_info = self.node_info_dict.get(node_guid)
            if node_info:
                node_info.shutdown_node()

    def register_node(self, node_details=None):
        node_guid = uuid.uuid4()
        with self.node_info_dict_lock:
            self.node_info_dict[node_guid] = NodeInfo(node_guid, 0, node_details)
        return node_guid

    def enqueue_job(self, inner_simulations, job_details=None):
        job = Job(inner_simulations, job_details)
        with self.jobs_dict_lock:
            self.jobs_dict[job.job_guid] = job
        with self.primary_queue_lock:
            self.primary_queue.append(job)
            self.total_simulation_count += len(inner_simulations)
        return job.job_guid

    def try_get_pending_simulation(self):
        with self.primary_queue_lock:
            while self.current_job_idx < len(self.primary_queue):
                current_job = self.primary_queue[self.current_job_idx]
                if current_job.get_next_simulation_record():
                    return True
                else:
                    self.current_job_idx += 1
        return False

    def try_get_retry_simulation(self):
        return self.retry_queue.get_nowait() if not self.retry_queue.empty() else None

    def identify_timed_out_outstanding_work(self):
        with self.outstanding_work_dict_lock:
            for work_guid, work_record in list(self.outstanding_work_dict.items()):
                sim_record = work_record.sim_record
                sim_results = self.jobs_dict[sim_record.job_guid].get_simulation_results(sim_record.sim_idx, work_guid)
                if sim_results.check_and_mark_timeout():
                    if sim_record.retry_count < MagicNumbers.error_count_until_failure:
                        self.retry_queue.put(sim_record)
                    else:
                        self.failed_simulation_count += 1
                    self.total_simulation_error_count += 1
                    del self.outstanding_work_dict[work_guid]

    def dispatch_simulation(self, simulation_record, node_guid):
        work_guid = uuid.uuid4()
        with self.node_info_dict_lock:
            node_info = self.node_info_dict[node_guid]
            node_info.start_new_work(work_guid)
        
        with self.outstanding_work_dict_lock:
            self.outstanding_work_dict[work_guid] = DispatchedWorkRecord(work_guid, node_guid, simulation_record)

        job = self.jobs_dict[simulation_record.job_guid]
        inner_simulation = job.get_inner_sim(simulation_record.sim_idx)

        self.dispatched_simulation_count += 1
        return SimulationDetails(work_guid, inner_simulation, 0)

    def get_simulation(self, node_guid):
        if self.try_get_pending_simulation():
            return self.dispatch_simulation(self.pending_sim_record, node_guid)
        elif sim_record := self.try_get_retry_simulation():
            return self.dispatch_simulation(sim_record, node_guid)

        self.identify_timed_out_outstanding_work()
        if sim_record := self.try_get_retry_simulation():
            return self.dispatch_simulation(sim_record, node_guid)

        return SimulationDetails(None, None, MagicNumbers.default_retry_delay_seconds)

    def work_finished(self, work_guid):
        with self.outstanding_work_dict_lock:
            if work_guid not in self.outstanding_work_dict:
                return False
            work_record = self.outstanding_work_dict.pop(work_guid)

        with self.node_info_dict_lock:
            self.node_info_dict[work_record.node_guid].finish_work()

        job = self.jobs_dict[work_record.sim_record.job_guid]
        sim_results = job.get_simulation_results(work_record.sim_record.sim_idx, work_guid)
        sim_results.results_status = ResultsStatuses.COMPLETE

        self.total_finished_simulation_count += 1
        return True

    def record_error(self, work_guid, error_details=None):
        with self.outstanding_work_dict_lock:
            if work_guid not in self.outstanding_work_dict:
                return False
            work_record = self.outstanding_work_dict.pop(work_guid)

        with self.node_info_dict_lock:
            node_info = self.node_info_dict[work_record.node_guid]
            node_error_count = node_info.error_work()

        job = self.jobs_dict[work_record.sim_record.job_guid]
        sim_results = job.get_simulation_results(work_record.sim_record.sim_idx, work_guid)
        sim_results.results_status = ResultsStatuses.FAILED
        sim_results.error_details = error_details

        self.failed_simulation_count += 1
        self.total_simulation_error_count += 1
        return True

    def append_results(self, work_guid, incremental_results):
        with self.outstanding_work_dict_lock:
            if work_guid not in self.outstanding_work_dict:
                return False
            work_record = self.outstanding_work_dict[work_guid]

        with self.node_info_dict_lock:
            self.node_info_dict[work_record.node_guid].update_work()

        job = self.jobs_dict[work_record.sim_record.job_guid]
        sim_results = job.get_simulation_results(work_record.sim_record.sim_idx, work_guid)
        sim_results.append_results(incremental_results)

        self.total_results_received_count += 1
        return True

import threading
import uuid
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

class JobRepository:
    def __init__(self):
        self.jobs_dict_lock = threading.Lock()
        self.jobs_dict = {}

        self.primary_queue_lock = threading.Lock()
        self.primary_queue = []

        self.current_job_idx = 0
        self.total_simulation_count = 0
        self.dispatched_simulation_count = 0
        self.total_simulation_error_count = 0
        self.failed_simulation_count = 0
        self.total_results_received_count = 0
        self.total_finished_simulation_count = 0

        self.outstanding_work_dict_lock = threading.Lock()
        self.outstanding_work_dict = {}

        self.retry_queue = Queue()

        self.node_info_dict_lock = threading.Lock()
        self.node_info_dict = {}

    def shutdown_node(self, node_guid):
        with self.node_info_dict_lock:
            node_info = self.node_info_dict.get(node_guid)
            if node_info:
                node_info.shutdown_node()

    def register_node(self, node_details=None):
        node_guid = uuid.uuid4()
        with self.node_info_dict_lock:
            self.node_info_dict[node_guid] = NodeInfo(node_guid, 0, node_details)
        return node_guid

    def enqueue_job(self, inner_simulations, job_details=None):
        job = Job(inner_simulations, job_details)
        with self.jobs_dict_lock:
            self.jobs_dict[job.job_guid] = job
        with self.primary_queue_lock:
            self.primary_queue.append(job)
            self.total_simulation_count += len(inner_simulations)
        return job.job_guid

    def try_get_pending_simulation(self):
        with self.primary_queue_lock:
            while self.current_job_idx < len(self.primary_queue):
                current_job = self.primary_queue[self.current_job_idx]
                if current_job.get_next_simulation_record():
                    return True
                else:
                    self.current_job_idx += 1
        return False

    def try_get_retry_simulation(self):
        return self.retry_queue.get_nowait() if not self.retry_queue.empty() else None

    def identify_timed_out_outstanding_work(self):
        with self.outstanding_work_dict_lock:
            for work_guid, work_record in list(self.outstanding_work_dict.items()):
                sim_record = work_record.sim_record
                sim_results = self.jobs_dict[sim_record.job_guid].get_simulation_results(sim_record.sim_idx, work_guid)
                if sim_results.check_and_mark_timeout():
                    if sim_record.retry_count < MagicNumbers.error_count_until_failure:
                        self.retry_queue.put(sim_record)
                    else:
                        self.failed_simulation_count += 1
                    self.total_simulation_error_count += 1
                    del self.outstanding_work_dict[work_guid]

    def dispatch_simulation(self, simulation_record, node_guid):
        work_guid = uuid.uuid4()
        with self.node_info_dict_lock:
            node_info = self.node_info_dict[node_guid]
            node_info.start_new_work(work_guid)
        
        with self.outstanding_work_dict_lock:
            self.outstanding_work_dict[work_guid] = DispatchedWorkRecord(work_guid, node_guid, simulation_record)

        job = self.jobs_dict[simulation_record.job_guid]
        inner_simulation = job.get_inner_sim(simulation_record.sim_idx)

        self.dispatched_simulation_count += 1
        return SimulationDetails(work_guid, inner_simulation, 0)

    def get_simulation(self, node_guid):
        if self.try_get_pending_simulation():
            return self.dispatch_simulation(self.pending_sim_record, node_guid)
        elif sim_record := self.try_get_retry_simulation():
            return self.dispatch_simulation(sim_record, node_guid)

        self.identify_timed_out_outstanding_work()
        if sim_record := self.try_get_retry_simulation():
            return self.dispatch_simulation(sim_record, node_guid)

        return SimulationDetails(None, None, MagicNumbers.default_retry_delay_seconds)

    def work_finished(self, work_guid):
        with self.outstanding_work_dict_lock:
            if work_guid not in self.outstanding_work_dict:
                return False
            work_record = self.outstanding_work_dict.pop(work_guid)

        with self.node_info_dict_lock:
            self.node_info_dict[work_record.node_guid].finish_work()

        job = self.jobs_dict[work_record.sim_record.job_guid]
        sim_results = job.get_simulation_results(work_record.sim_record.sim_idx, work_guid)
        sim_results.results_status = ResultsStatuses.COMPLETE

        self.total_finished_simulation_count += 1
        return True

    def record_error(self, work_guid, error_details=None):
        with self.outstanding_work_dict_lock:
            if work_guid not in self.outstanding_work_dict:
                return False
            work_record = self.outstanding_work_dict.pop(work_guid)

        with self.node_info_dict_lock:
            node_info = self.node_info_dict[work_record.node_guid]
            node_error_count = node_info.error_work()

        job = self.jobs_dict[work_record.sim_record.job_guid]
        sim_results = job.get_simulation_results(work_record.sim_record.sim_idx, work_guid)
        sim_results.results_status = ResultsStatuses.FAILED
        sim_results.error_details = error_details

        self.failed_simulation_count += 1
        self.total_simulation_error_count += 1
        return True

    def append_results(self, work_guid, incremental_results):
        with self.outstanding_work_dict_lock:
            if work_guid not in self.outstanding_work_dict:
                return False
            work_record = self.outstanding_work_dict[work_guid]

        with self.node_info_dict_lock:
            self.node_info_dict[work_record.node_guid].update_work()

        job = self.jobs_dict[work_record.sim_record.job_guid]
        sim_results = job.get_simulation_results(work_record.sim_record.sim_idx, work_guid)
        sim_results.append_results(incremental_results)

        self.total_results_received_count += 1
        return True

    def export_job_summaries_file(self, file_name, jobs):
        job_summaries = [job.export_job_summary() for job in jobs]
        with open(file_name, 'w', encoding='utf-8') as file_out:
            json.dump(job_summaries, file_out)

    def export_completed_to_folder(self, folder, compression=True):
        os.makedirs(folder, exist_ok=True)
        jobs = self.get_jobs_array()

        job_summaries_file = os.path.join(folder, 'jobSummaries.json')
        self.export_job_summaries_file(job_summaries_file, jobs)

        def export_job(job):
            job.export_completed_simulations_folder(folder, compression=compression)

        with ThreadPoolExecutor() as executor:
            executor.map(export_job, jobs)

    def export_all_to_folder(self, folder, compression=True):
        os.makedirs(folder, exist_ok=True)
        jobs = self.get_jobs_array()

        job_summaries_file = os.path.join(folder, 'jobSummaries.json')
        self.export_job_summaries_file(job_summaries_file, jobs)

        def export_job(job):
            job.export_all_simulations_folder(folder, compression=compression)

        with ThreadPoolExecutor() as executor:
            executor.map(export_job, jobs)

    def export_nodes(self):
        with self.node_info_dict_lock:
            return [node_info.export() for node_info in self.node_info_dict.values()]

    def get_stats(self):
        with self.outstanding_work_dict_lock:
            outstanding_simulation_count = len(self.outstanding_work_dict)

        with self.node_info_dict_lock:
            total_node_count = len(self.node_info_dict)
            active_node_count = sum(node_info.is_active() for node_info in self.node_info_dict.values())

        return {
            'TotalJobCount': len(self.primary_queue),
            'DispatchedJobCount': self.current_job_idx,
            'TotalSimulationCount': self.total_simulation_count,
            'DispatchedSimulationCount': self.dispatched_simulation_count,
            'OutstandingSimulationCount': outstanding_simulation_count,
            'TotalSimulationErrorCount': self.total_simulation_error_count,
            'FailedSimulationCount': self.failed_simulation_count,
            'TotalResultsReceivedCount': self.total_results_received_count,
            'TotalFinishedSimulationCount': self.total_finished_simulation_count,
            'TotalNodeCount': total_node_count,
            'ActiveNodeCount': active_node_count,
            'SimulationsInRetryQueue': self.retry_queue.qsize()
        }

    def get_jobs_array(self):
        with self.jobs_dict_lock:
            return list(self.jobs_dict.values())

