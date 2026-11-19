from ai_bot.agent.nodes import (
    validation_node
)
from tests.test_state import (
    validation_node_state_example
)


def test_nodes():
    result = validation_node(validation_node_state_example)
    print("Validation Node Result:", result)


if __name__ == "__main__":
    # Run the test function directly
    test_nodes()
