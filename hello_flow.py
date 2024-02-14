from metaflow import FlowSpec, step, pypi

class HelloWorldFlow(FlowSpec):
    """
    This flow prints two lines!

    """

    @step
    def start(self):
        """
        This is the 'start' step. All flows must have a step named 'start' that
        is the first step in the flow.

        """
        print("Wello horld!")
        self.next(self.end)

    @step
    def end(self):
        """
        This is the 'end' step. All flows must have an 'end' step, which is the
        last step in the flow.

        """
        print("Success!")


if __name__ == "__main__":
    HelloWorldFlow()