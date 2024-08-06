class Tool:
    def __init__(self, data=None, call=None):
        self.data = data
        self.call = call

    def execute(self, *args, **kwargs):
        if self.call:
            return self.call(*args, **kwargs)
        else:
            raise NotImplementedError("No call function defined for this tool.")

class ToolRegistry:
    def __init__(self):
        self.tools = {}

    def register_tool(self, name, tool):
        self.tools[name] = tool

    def get_tool(self, name):
        return self.tools.get(name)
    
    def get_all_tool_names(self):
        return self.tools.keys()
    
    def get_all_tool_datas(self):
        return [self.get_tool(key).data for key in self.tools]
