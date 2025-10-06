#pragma once

#include <string>
#include <system_error>

namespace marble {

enum class StatusCode {
    kOk = 0,
    kNotFound = 1,
    kCorruption = 2,
    kIoError = 3,
    kInvalidArgument = 4,
    kWriteConflict = 5,
    kCompactionError = 6,
    kOutOfMemory = 7,
    kTimeout = 8,
    kAborted = 9,
    kResourceExhausted = 10,
    kDeadlineExceeded = 11,
    kNotImplemented = 12,
    kAlreadyExists = 13,
    kInternalError = 14,
};

class Status {
public:
    Status() : code_(StatusCode::kOk) {}
    Status(StatusCode code) : code_(code) {}
    Status(StatusCode code, const std::string& message)
        : code_(code), message_(message) {}

    static Status OK() { return Status(); }
    static Status NotFound(const std::string& message = "") {
        return Status(StatusCode::kNotFound, message);
    }
    static Status Corruption(const std::string& message = "") {
        return Status(StatusCode::kCorruption, message);
    }
    static Status IOError(const std::string& message = "") {
        return Status(StatusCode::kIoError, message);
    }
    static Status InvalidArgument(const std::string& message = "") {
        return Status(StatusCode::kInvalidArgument, message);
    }
    static Status WriteConflict(const std::string& message = "") {
        return Status(StatusCode::kWriteConflict, message);
    }

    static Status ResourceExhausted(const std::string& message = "") {
        return Status(StatusCode::kResourceExhausted, message);
    }

    static Status DeadlineExceeded(const std::string& message = "") {
        return Status(StatusCode::kDeadlineExceeded, message);
    }

    static Status NotImplemented(const std::string& message = "") {
        return Status(StatusCode::kNotImplemented, message);
    }
    static Status AlreadyExists(const std::string& message = "") {
        return Status(StatusCode::kAlreadyExists, message);
    }
    static Status InternalError(const std::string& message = "") {
        return Status(StatusCode::kInternalError, message);
    }

    bool ok() const { return code_ == StatusCode::kOk; }
    bool IsNotFound() const { return code_ == StatusCode::kNotFound; }
    bool IsCorruption() const { return code_ == StatusCode::kCorruption; }
    bool IsIOError() const { return code_ == StatusCode::kIoError; }
    bool IsInvalidArgument() const { return code_ == StatusCode::kInvalidArgument; }
    bool IsWriteConflict() const { return code_ == StatusCode::kWriteConflict; }
    bool IsAlreadyExists() const { return code_ == StatusCode::kAlreadyExists; }
    bool IsInternalError() const { return code_ == StatusCode::kInternalError; }

    StatusCode code() const { return code_; }
    const std::string& message() const { return message_; }

    std::string ToString() const;

private:
    StatusCode code_;
    std::string message_;
};

inline std::string Status::ToString() const {
    std::string result;
    switch (code_) {
        case StatusCode::kOk:
            result = "OK";
            break;
        case StatusCode::kNotFound:
            result = "NotFound";
            break;
        case StatusCode::kCorruption:
            result = "Corruption";
            break;
        case StatusCode::kIoError:
            result = "IOError";
            break;
        case StatusCode::kInvalidArgument:
            result = "InvalidArgument";
            break;
        case StatusCode::kWriteConflict:
            result = "WriteConflict";
            break;
        case StatusCode::kCompactionError:
            result = "CompactionError";
            break;
        case StatusCode::kOutOfMemory:
            result = "OutOfMemory";
            break;
        case StatusCode::kTimeout:
            result = "Timeout";
            break;
        case StatusCode::kAborted:
            result = "Aborted";
            break;
        case StatusCode::kResourceExhausted:
            result = "ResourceExhausted";
            break;
        case StatusCode::kDeadlineExceeded:
            result = "DeadlineExceeded";
            break;
        case StatusCode::kNotImplemented:
            result = "NotImplemented";
            break;
        case StatusCode::kAlreadyExists:
            result = "AlreadyExists";
            break;
        case StatusCode::kInternalError:
            result = "InternalError";
            break;
    }

    if (!message_.empty()) {
        result += ": " + message_;
    }

    return result;
}

} // namespace marble
