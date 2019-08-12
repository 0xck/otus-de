import logging
from abc import abstractmethod
from functools import reduce
from os import getenv
from typing import Any, Optional, Union, Tuple

import aerospike


class ClientError(Exception):
    pass


class ClientOptionExists(ClientError):
    pass


class ClientConnectError(ClientError):
    pass


class AerospikeClientUnavailable(ClientError):
    pass


class Client:
    def __init__(self, config: dict = {}):
        self._config = config or {}
        self._client = None

    @property
    def handler(self) -> aerospike.Client:
        if not self.is_closed():
            return self._client

        raise AerospikeClientUnavailable()

    def with_option(self, option: str, value: Any) -> "Client":
        if option in self._config:
            raise ClientOptionExists(option)

        self._config[option] = value

        return self

    def with_options(self, options: dict) -> "Client":
        return reduce(lambda _, option_kv: self.with_option(*option_kv), options.items(), self)

    def connect(self) -> "Client":
        try:
            if not self.is_available():
                self._client = aerospike.client(self._config).connect()
                return self

            if self._client.is_connected():
                return self

            raise AerospikeClientUnavailable()

        except aerospike.exception.ClientError as exc:
            raise ClientConnectError() from exc

    def is_available(self) -> bool:
        return self._client is not None

    def is_closed(self) -> bool:
        if not self.is_available():
            raise AerospikeClientUnavailable()

        if self.is_available and self._client.is_connected():
            return False

        return True

    def close(self) -> None:
        if self.is_available and self._client.is_connected():
            self._client.close()

    def __repr__(self) -> str:
        return f"AerospikeClientWrapper, client: <{self._client}>"


class WrongNumberFormat(Exception):
    pass


class PhonePolicy:
    @staticmethod
    @abstractmethod
    def check(phone: str) -> Tuple[bool, str]:
        pass


class PhonePolicyPipeline(PhonePolicy):
    def __init__(self, policy: PhonePolicy, *policies: PhonePolicy):
        self._policies = (policy, ) + policies

    def check(self, phone: str) -> Tuple[bool, str]:
        for policy in self._policies:
            (result, error_message) = policy.check(phone)
            if not result:
                return result, error_message

        return True, ""


class NonEmptyPhone(PhonePolicy):
    @staticmethod
    def check(phone: str) -> Tuple[bool, str]:
        if phone:
            return True, ""

        return False, "empty phone number"


class OnlyPrintablePhone(PhonePolicy):
    @staticmethod
    def check(phone: str) -> Tuple[bool, str]:
        if phone.isprintable():
            return True, ""

        return False, "non-printable characters in phone number"


class PhoneNumber:
    def __init__(self, phone: str, policy: PhonePolicy):
        (valid_phone, error_message) = policy.check(phone)

        if not valid_phone:
            raise WrongNumberFormat(error_message)

        self.phone = phone

    def __repr__(self) -> str:
        return f"PhoneNumber: <{self.phone}>"

    def __str__(self) -> str:
        return f"{self.phone}"


class WrongCustomerID(Exception):
    pass


class CustomerID:
    def __init__(self, customer_id: int):
        if customer_id < 1:
            raise WrongCustomerID(str(customer_id))
        self.id = customer_id

    def __repr__(self) -> str:
        return f"CustomerID: <{self.id}>"

    def __str__(self) -> str:
        return f"{self.id}"


class WrongLTV(Exception):
    pass


class LTV:
    def __init__(self, ltv: int):
        if ltv < 0:
            raise WrongLTV(f"LTV has to be more than 0, given is {ltv}")

        self.ltv = ltv

    def __repr__(self) -> str:
        return f"LTV: <{self.ltv}>"


def _add_customer(client: Client, namespace: str, set_: str,
                  customer_id: CustomerID, phone_number: PhoneNumber, lifetime_value: LTV) -> CustomerID:
    key = (namespace, set_, customer_id.id)
    bins = {"phone": phone_number.phone, "ltv": lifetime_value.ltv}

    client.handler.put(key, bins)
    return customer_id


def _get_ltv_by_id(client: Client, namespace: str, set_: str,
                   customer_id: CustomerID) -> Optional[LTV]:

    key = (namespace, set_, customer_id.id)
    # defining ltv due following exception handling
    ltv: Optional[int] = None

    try:
        (_, _, bins) = client.handler.select(key, ["ltv"])
        ltv = bins.get("ltv")

        if ltv is None:
            logging.error(f"Non-exist ltv for customer {customer_id}")
            return None

        return LTV(ltv)

    except aerospike.exception.RecordNotFound:
        logging.error(f"Requested non-existent customer {customer_id}")
        return None

    except WrongLTV:
        logging.error(f"Broken ltv value <{ltv}> for customer {customer_id}")
        return None


def _get_ltv_by_phone(client: Client, namespace: str, set_: str,
                      phone_number: PhoneNumber) -> Optional[LTV]:

    query = client.handler.query(namespace, set_)
    query.select("ltv")
    query.where(aerospike.predicates.equals("phone", phone_number.phone))
    results = query.results()

    if not results:
        logging.error(f"Requested phone number {phone_number} is not found")
        return None

    (_, _, bins) = results[0]
    ltv = bins.get("ltv")

    if ltv is None:
        logging.error(f"Non-exist ltv for phone {phone_number}")
        return None

    try:
        return LTV(ltv)

    except WrongLTV:
        logging.error(f"Broken ltv value <{ltv}> for phone {phone_number}")
        return None


NAMESPACE = getenv("LTV_SERVICE_NAMESPACE", "test").strip()
SET = getenv("LTV_SERVICE_SET", "customers").strip()
PHONE_CHECK_POLICY = PhonePolicyPipeline(NonEmptyPhone(), OnlyPrintablePhone())


def get_client_settings():
    hosts = [(h.split(":")[0], int(h.split(":")[1])) for h in (getenv("LTV_SERVICE_HOSTS", "127.0.0.1:3000")).strip().split(",")]
    timeout = int(getenv("LTV_SERVICE_TIMEOUT", "1000").strip())

    return {"hosts": hosts, "policies": {"timeout": timeout}}


def add_customer(client: Client, customer_id: int, phone_number: str, lifetime_value: int) -> int:
    phone = PhoneNumber(phone_number, PHONE_CHECK_POLICY)
    id_ = CustomerID(customer_id)
    ltv = LTV(lifetime_value)
    result = _add_customer(client, NAMESPACE, SET, id_, phone, ltv).id

    assert (result == customer_id), AssertionError(f"Result id {result} is not equal given customer id {customer_id}")

    return result


def get_ltv_by_id(client: Client, customer_id: int) -> Optional[int]:
    id_ = CustomerID(customer_id)
    ltv = _get_ltv_by_id(client, NAMESPACE, SET, id_)

    return ltv.ltv if ltv is not None else None


def get_ltv_by_phone(client: Client, phone_number: str) -> Optional[int]:
    phone = PhoneNumber(phone_number, PHONE_CHECK_POLICY)
    ltv = _get_ltv_by_phone(client, NAMESPACE, SET, phone)

    return ltv.ltv if ltv is not None else None
