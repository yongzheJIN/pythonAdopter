from abc import ABC, abstractmethod


class FunctionHandler(ABC):
    @abstractmethod
    def funcDelete(self, data, useReplace):
        """
        抽象方法：删除操作的方法
        :param data: {
                    table:表名,
                    primary_list:[{"name":"","value":""}]
                    database:库名
                    }
        :param useReplace: 是否使用替换逻辑
        """
        pass

    @abstractmethod
    def funcUpdate(self, data, useReplace):
        """
        抽象方法：更新操作的方法
        :param data: {
                    table:表名,
                    primary_list:[{"name":"","value":""}]
                    database:库名
                    before:{"name":"1"},
                    after:{"name":"2"}
                    }
        :param useReplace: 是否使用替换逻辑
        """
        pass

    @abstractmethod
    def funcInsert(self, data, useReplace):
        """
        抽象方法：插入操作的方法
        :param data: {
                    table:表名,
                    primary_list:[{"name":"","value":""}]
                    database:库名
                    after:{"name":"2"}
                    }
        :param useReplace: 是否使用替换逻辑
        """
        pass
