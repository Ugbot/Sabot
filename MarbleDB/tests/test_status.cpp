#include <gtest/gtest.h>
#include <marble/status.h>

namespace marble {

class StatusTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(StatusTest, DefaultConstructor) {
    Status status;
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(status.code(), StatusCode::kOk);
    EXPECT_TRUE(status.message().empty());
}

TEST_F(StatusTest, StatusCodeConstructor) {
    Status status(StatusCode::kNotFound);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), StatusCode::kNotFound);
    EXPECT_TRUE(status.message().empty());
}

TEST_F(StatusTest, StatusWithMessage) {
    std::string message = "test error message";
    Status status(StatusCode::kInvalidArgument, message);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), StatusCode::kInvalidArgument);
    EXPECT_EQ(status.message(), message);
}

TEST_F(StatusTest, StaticConstructors) {
    Status ok = Status::OK();
    EXPECT_TRUE(ok.ok());

    Status not_found = Status::NotFound("key not found");
    EXPECT_TRUE(not_found.IsNotFound());
    EXPECT_EQ(not_found.message(), "key not found");

    Status io_error = Status::IOError("disk full");
    EXPECT_TRUE(io_error.IsIOError());
    EXPECT_EQ(io_error.message(), "disk full");
}

TEST_F(StatusTest, ToString) {
    Status ok = Status::OK();
    EXPECT_EQ(ok.ToString(), "OK");

    Status error = Status::Corruption("data corrupted");
    EXPECT_EQ(error.ToString(), "Corruption: data corrupted");
}

} // namespace marble
