class TaskObject:
    def __init__(self, task_id: int, user: str, command: str, uuid: str, directory: str, env: dict, date_str: str):
        """
        :param command: (String)
        :param uuid: (String)
        :param directory: (String)
        :param env: (Dict)
        :param date_str: (String) yyyy-MM-dd
        """
        self.task_id = task_id
        self.user = user
        self.command = command
        self.uuid = uuid
        self.directory = directory
        self.env = env
        self.date_str = date_str

    def to_dict(self):
        """
        :return: Dict include Payload data
        """
        return {
            "task_id": self.task_id,
            "user": self.user,
            "command": self.command,
            "uuid": self.uuid,
            "directory": self.directory,
            "env": self.env,
            "date_str": self.date_str
        }

    def __repr__(self):
        """
        :return: Serialized String payload data
        """
        return f"Payload(task_id='{self.task_id}', user='{self.user}', command='{self.command}', uuid='{self.uuid}', directory='{self.directory}', env='{self.env}', date_str='{self.date_str}')"
