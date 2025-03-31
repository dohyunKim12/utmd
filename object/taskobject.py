from dataclasses import dataclass, asdict

@dataclass
class TaskObject:
    task_id: int
    job_id: int
    user: str
    command: str
    uuid: str
    directory: str
    env: dict
    date_str: str  # yyyy-MM-dd
    license_type: str
    license_count: int
    timelimit: int
    uid: int
    gid: int
    requested_cpu: int = 0

    def to_dict(self):
        return asdict(self)
