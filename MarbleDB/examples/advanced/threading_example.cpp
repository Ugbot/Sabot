#include <marble/marble.h>
#include <iostream>
#include <vector>
#include <chrono>
#include <memory>

int main() {
    std::cout << "MarbleDB Threading and Filesystem Example" << std::endl;
    std::cout << "==========================================" << std::endl;

    // Create a task scheduler with 4 threads
    marble::TaskScheduler scheduler(4);
    std::cout << "Created task scheduler with " << scheduler.GetThreadCount() << " threads" << std::endl;

    // Create some tasks
    std::vector<std::shared_ptr<marble::Task>> tasks;

    for (int i = 0; i < 10; ++i) {
        tasks.push_back(marble::MakeTask([i]() {
            std::cout << "Task " << i << " executing on thread "
                      << std::this_thread::get_id() << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(100 + i * 10));
            std::cout << "Task " << i << " completed" << std::endl;
        }));
    }

    // Schedule tasks
    auto producer = scheduler.CreateProducer();
    std::cout << "Scheduling " << tasks.size() << " tasks..." << std::endl;
    scheduler.ScheduleTasks(*producer, tasks);

    // Wait for completion
    std::cout << "Waiting for tasks to complete..." << std::endl;
    scheduler.WaitForCompletion();
    std::cout << "All tasks completed!" << std::endl;

    // Test filesystem
    std::cout << "\nTesting filesystem..." << std::endl;
    auto fs = std::shared_ptr<marble::FileSystem>(marble::FileSystem::CreateLocal());
    std::cout << "Created filesystem: " << fs->GetName() << std::endl;

    // Test file operations
    std::string test_file = "/tmp/marble_test.txt";
    std::string test_content = "Hello, MarbleDB! This is a test file.";

    // Write file
    marble::FileOperations file_ops(fs);
    auto buffer = std::make_shared<arrow::Buffer>(
        reinterpret_cast<const uint8_t*>(test_content.data()),
        test_content.size()
    );

    auto status = file_ops.WriteFile(test_file, buffer);
    if (status.ok()) {
        std::cout << "Successfully wrote file: " << test_file << std::endl;
    } else {
        std::cout << "Failed to write file: " << status.ToString() << std::endl;
        return 1;
    }

    // Read file back
    std::shared_ptr<arrow::Buffer> read_buffer;
    status = file_ops.ReadFile(test_file, &read_buffer);
    if (status.ok()) {
        std::string read_content(
            reinterpret_cast<const char*>(read_buffer->data()),
            read_buffer->size()
        );
        std::cout << "Successfully read file content: " << read_content << std::endl;

        if (read_content == test_content) {
            std::cout << "File content matches!" << std::endl;
        } else {
            std::cout << "File content mismatch!" << std::endl;
        }
    } else {
        std::cout << "Failed to read file: " << status.ToString() << std::endl;
        return 1;
    }

    // Test file info
    marble::FileInfo info;
    status = fs->GetFileInfo(test_file, &info);
    if (status.ok()) {
        std::cout << "File info - Size: " << info.size << " bytes, Regular file: "
                  << (info.is_regular_file ? "yes" : "no") << std::endl;
    }

    // Clean up
    status = fs->RemoveFile(test_file);
    if (status.ok()) {
        std::cout << "Successfully removed test file" << std::endl;
    }

    std::cout << "\nExample completed successfully!" << std::endl;
    return 0;
}
