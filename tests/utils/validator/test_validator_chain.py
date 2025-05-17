"""
    Unit tests for the ValidatorChain class.
"""

import pytest

from src.utils.validator.expression_validator import ExpressionValidator
from src.utils.validator.name_validator import NameValidator
from src.utils.validator.reference_validator import ReferenceValidator
from src.utils.validator.validator_chain import ValidatorChain


@pytest.fixture(name="validator_chain")
def fixture_validator_chain():
    """
    Fixture to create a ValidatorChain instance with the required validators.
    """
    # Create a ValidatorChain instance with the validators
    chain = ValidatorChain()
    chain.add_validator(ExpressionValidator())
    chain.add_validator(ReferenceValidator())
    chain.add_validator(NameValidator())

    return chain


class TestValidatorChain:
    """
    This class contains unit tests to validate the behavior of the ValidatorChain class.
    """

    def test_validator_chain(self, spark, validator_chain):
        """
        Test the creation and basic attributes of the ValidatorChain instance.

        Validates that the ValidatorChain instance is created successfully and
        has the required methods.
        """
        assert validator_chain is not None
        assert hasattr(validator_chain, "validate")
        assert hasattr(validator_chain, "add_validator")

    def test_validators_in_chain(self, validator_chain):
        """
        Test the validators added to the ValidatorChain instance.

        Validates that the ValidatorChain contains the correct sequence of validators.
        """
        assert isinstance(validator_chain.validators[0], ExpressionValidator)
        assert isinstance(validator_chain.validators[1], ReferenceValidator)
        assert isinstance(validator_chain.validators[2], NameValidator)
