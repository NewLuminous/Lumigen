from __future__ import annotations
import re


class InvalidTopicException(Exception):
    pass


"""
    Reference: https://github.com/apache/kafka/blob/3.9.0/clients/src/main/java/org/apache/kafka/common/internals/Topic.java
"""
class TopicValidator:
    GROUP_METADATA_TOPIC_NAME = "__consumer_offsets"
    TRANSACTION_STATE_TOPIC_NAME = "__transaction_state"
    SHARE_GROUP_STATE_TOPIC_NAME = "__share_group_state"
    CLUSTER_METADATA_TOPIC_NAME = "__cluster_metadata"
    INTERNAL_TOPICS = set([GROUP_METADATA_TOPIC_NAME, TRANSACTION_STATE_TOPIC_NAME, SHARE_GROUP_STATE_TOPIC_NAME])

    LEGAL_CHARS = "[a-zA-Z0-9._-]"
    MAX_NAME_LENGTH = 249

    @classmethod
    def validate(cls, topic: str) -> None:
        reason_invalid = cls.detect_invalid_topic(topic)
        if reason_invalid:
            raise InvalidTopicException(f"Topic name is invalid: {reason_invalid}")
        
    @classmethod
    def is_valid(cls, topic: str) -> bool:
        reason_invalid = cls.detect_invalid_topic(topic)
        return reason_invalid is None
    
    @classmethod
    def detect_invalid_topic(cls, name: str) -> str:
        if len(name) == 0:
            return "the empty string is not allowed"
        if name == '.':
            return "'.' is not allowed"
        if name == "..":
            return "'..' is not allowed"
        if len(name) > cls.MAX_NAME_LENGTH:
            return "the length of '" + name + "' is longer than the max allowed length " + MAX_NAME_LENGTH
        if not cls.contains_valid_pattern(name):
             return "'" + name + "' contains one or more characters other than " +\
                "ASCII alphanumerics, '.', '_' and '-'"
        return None
    
    @classmethod
    def is_internal(cls, topic: str) -> bool:
        return topic in cls.INTERNAL_TOPICS
    
    @staticmethod
    def has_collision_chars(topic: str) -> bool:
        return '_' in topic or '.' in topic
    
    @classmethod
    def contains_valid_pattern(cls, topic: str) -> bool:
        return bool(re.fullmatch(f"^{cls.LEGAL_CHARS}+$", topic))
