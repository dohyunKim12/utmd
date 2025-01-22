class Payload:
    def __init__(self, task_id: int, user: str, command: str, uuid: str, directory: str, env:dict):
        """
        :param command: (String)
        :param uuid: (String)
        :param directory: (String)
        :param env: (Dict)
        """
        self.task_id = task_id
        self.user = user
        self.command = command
        self.uuid = uuid
        self.directory = directory
        self.env = env

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
            "env": self.env
        }

    def __repr__(self):
        """
        :return: Serialized String payload data
        """
        return f"Payload(task_id='{self.task_id}', user='{self.user}', command='{self.command}', uuid='{self.uuid}', directory='{self.directory}', env='{self.env}')"
