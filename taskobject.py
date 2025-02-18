class TaskObject:
    def __init__(self, task_id: int, job_id: int, user: str, command: str, uuid: str, directory: str, env: dict, date_str: str, license_type: str, license_count: int, timelimit: int, requested_cpu: int = 0):
        """
        :param command: (String)
        :param uuid: (String)
        :param directory: (String)
        :param env: (Dict)
        :param date_str: (String) yyyy-MM-dd
        """
        self.task_id = task_id
        self.job_id = job_id
        self.user = user
        self.command = command
        self.uuid = uuid
        self.directory = directory
        self.env = env
        self.date_str = date_str
        self.license_type = license_type
        self.license_count = license_count
        self.timelimit = timelimit
        self.requested_cpu = requested_cpu

    def to_dict(self):
        """
        :return: Dict include Payload data
        """
        return {
            "task_id": self.task_id,
            "job_id": self.job_id,
            "user": self.user,
            "command": self.command,
            "uuid": self.uuid,
            "directory": self.directory,
            "env": self.env,
            "date_str": self.date_str,
            "license_type": self.license_type,
            "license_count": self.license_count,
            "timelimit": self.timelimit,
            "requested_cpu": self.requested_cpu
        }

    def __repr__(self):
        """
        :return: Serialized String payload data
        """
        return (f"Payload(task_id='{self.task_id}', job_id='{self.job_id}', user='{self.user}', command='{self.command}', uuid='{self.uuid}', directory='{self.directory}', env='{self.env}', "
                f"date_str='{self.date_str}', license_type='{self.license_type}', license_count='{self.license_count}', timelimit='{self.timelimit}', requested_cpu='{self.requested_cpu}')")
