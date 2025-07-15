import typing
import re
import json

class LogParser:
    """
    A class to parse log content and extract details of exceptions.
    """

    # Regex to identify a typical log line start (timestamp followed by level)
    # This helps in determining the end of an exception block if it's followed by new log entries.
    # Example 1: 2025-05-29 04:53:56,540 INFO ...
    # Example 2: 25/05/29 04:53:56 INFO ...
    LOG_LINE_START_PATTERN = re.compile(
        r"^(?:(?:\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d{3})|(?:\d{2}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}))\s+\w+"
    )
    TRACEBACK_HEADER = "Traceback (most recent call last):"
    # Common prefix for Python stack trace "File" lines
    PYTHON_FILE_LINE_PREFIX = "  File \"" 
    # Common prefix for Java stack trace lines
    JAVA_STACK_TRACE_PREFIX = "\tat "

    def parse_last_exception(self, log_content: str) -> typing.Optional[typing.Dict[str, str]]:
        """
        Parses the log content and extracts the type and message of the last exception.

        Args:
            log_content: A string containing the log data.

        Returns:
            A dictionary with 'type' and 'message' of the last exception,
            or None if no exception is found.
        """
        lines = log_content.splitlines()
        
        last_traceback_start_index = -1
        # Find the starting line index of the last traceback
        for i in range(len(lines) - 1, -1, -1):
            if lines[i].strip().startswith(self.TRACEBACK_HEADER):
                last_traceback_start_index = i
                break
        
        if last_traceback_start_index == -1:
            return None # No "Traceback..." header found

        # Collect all lines that could be part of this last exception's details
        current_exception_lines = []
        for i in range(last_traceback_start_index + 1, len(lines)):
            line = lines[i]
            # If we hit a new standard log line, the exception block has ended
            if self.LOG_LINE_START_PATTERN.match(line):
                break
            # If we hit another traceback header, this specific block has ended
            # (This is a safeguard, should not happen if we correctly found the *last* one)
            if line.strip().startswith(self.TRACEBACK_HEADER):
                break
            current_exception_lines.append(line)

        if not current_exception_lines:
            return None # Traceback header found, but no content after it

        # Now, from the collected lines, isolate the actual exception type and message
        # These are the lines *after* all the "  File ..." stack trace lines.
        exception_message_parts = []
        passed_initial_python_file_trace = False
        i = 0
        while i < len(current_exception_lines):
            line = current_exception_lines[i]

            # Skip stack frame lines until we find the actual exception message
            if not passed_initial_python_file_trace and line.startswith(self.PYTHON_FILE_LINE_PREFIX):
                i += 2
                continue
            
            # The first line not starting with "  File " (or "File ") is the start of the exception type/message
            passed_initial_python_file_trace = True

            # Skip lines that are Java-style stack traces (e.g., "\tat com.example...")
            if line.startswith(self.JAVA_STACK_TRACE_PREFIX):
                i += 1
                continue
            
            # Skip empty lines that might be within the traceback structure
            # but preserve lines that are empty but part of a multi-line message
            if line.strip() == "" and not exception_message_parts: # Allow empty lines once message started
                i += 1
                continue
                
            exception_message_parts.append(line)
            i += 1

        if not exception_message_parts:
            return None # Only stack trace lines found, no actual exception message

        # The first line of exception_message_parts contains the exception type and start of the message
        first_exception_line = exception_message_parts[0]
        
        # Split the first line into type and the first part of the message
        if ":" in first_exception_line:
            exception_type, message_start = first_exception_line.split(":", 1)
            exception_type = exception_type.strip()
            message_start = message_start.lstrip() # Remove leading space from message part
        else:
            # If no colon, assume the whole line is the exception type (less common for Python)
            exception_type = first_exception_line.strip()
            message_start = ""

        # Reconstruct the full message, including multi-line parts
        full_message_lines = [message_start]
        if len(exception_message_parts) > 1:
            # Add subsequent lines of the message, preserving their original leading/trailing whitespace
            # as they are part of the formatted message (e.g., SQL query plan)
            full_message_lines.extend(exception_message_parts[1:])
        
        full_message = "\n".join(full_message_lines).strip()

        return {
            "type": exception_type,
            "message": full_message
        }
    
    def parse_json_data_with_delimiters(
        self,
        log_content: str,
        start_delimiter: str,
        end_delimiter: str
    ) -> typing.Optional[typing.Any]:
        """
        Parses log content to find and extract a JSON object/array
        enclosed by specified start and end delimiters. It looks for the
        first occurrence of the start delimiter followed by the end delimiter.

        Args:
            log_content: The string content to parse.
            start_delimiter: The string marking the beginning of the JSON data.
            end_delimiter: The string marking the end of the JSON data.

        Returns:
            The parsed JSON data (e.g., dict, list), or None if delimiters
            are not found, data between them is empty/whitespace, or JSON parsing fails.
        """
        if not log_content:
            return None

        try:
            start_index_of_data_block = log_content.index(start_delimiter) + len(start_delimiter)
            end_index_of_data_block = log_content.index(end_delimiter, start_index_of_data_block)
            json_string_candidate = log_content[start_index_of_data_block:end_index_of_data_block].strip()
            if not json_string_candidate:
                return None
                
            parsed_json_data = json.loads(json_string_candidate)
            return parsed_json_data
            
        except ValueError:
            # This handles several cases:
            # 1. start_delimiter or end_delimiter not found (string.index raises ValueError)
            # 2. end_delimiter not found after start_delimiter (string.index raises ValueError)
            # 3. json.loads fails to parse json_string_candidate (json.JSONDecodeError is a subclass of ValueError)
            return None
        except Exception:
            return None
        
    def redact_delimited_content(
        self,
        log_content: str,
        start_delimiter: str,
        end_delimiter: str,
        replacement_text: str = "[REDACTED]"
    ) -> str:
        """
        Removes the content block (including delimiters) enclosed by specified
        start and end delimiters from the log content. If the block is found,
        it's replaced with `replacement_text`. If not found, original content is returned.
        This operates on the first found block.

        Args:
            log_content: The string content to process.
            start_delimiter: The string marking the beginning of the block to remove.
            end_delimiter: The string marking the end of the block to remove.
            replacement_text: Text to insert in place of the removed block.
                              Defaults to "[REDACTED]".
                              Set to "" to remove the block without replacement.

        Returns:
            The log content with the first delimited block removed/replaced,
            or the original log content if the block isn't found.
        """
        if not log_content:
            return ""

        try:
            start_block_index = log_content.index(start_delimiter)
            
            end_block_search_start_index = start_block_index + len(start_delimiter)
            end_delimiter_start_index = log_content.index(end_delimiter, end_block_search_start_index)
            end_block_index = end_delimiter_start_index + len(end_delimiter)
            
            # Reconstruct the string without the block
            return log_content[:start_block_index] + replacement_text + log_content[end_block_index:]
            
        except ValueError:
            return log_content
        except Exception:
            return log_content
    
class PySparkLogParser(LogParser):
    PYSPARK_PREFIX = "pyspark."

    def parse_last_exception(self, log_content: str) -> typing.Optional[typing.Dict[str, str]]:
        parsed_result = super().parse_last_exception(log_content)

        if parsed_result is None:
            return None

        exception_type = parsed_result.get("type")
        original_message = parsed_result.get("message", "") # Default to empty if key missing

        if exception_type and exception_type.startswith(self.PYSPARK_PREFIX):
            # This is a PySpark exception, try to truncate the message
            semicolon_index = original_message.find(";")
            if semicolon_index != -1:
                # Found a semicolon, truncate the message
                shortened_message = original_message[:semicolon_index].strip()
                return {
                    "type": exception_type,
                    "message": shortened_message
                }
            else:
                return parsed_result
        else:
            # Not a PySpark exception, or type was None.
            return parsed_result