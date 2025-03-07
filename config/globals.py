class Globals:
    running_flag = True
    srun_task_dict = {}
    logger = None

    @classmethod
    def set_logger(cls, logger_instance):
        cls.logger = logger_instance
