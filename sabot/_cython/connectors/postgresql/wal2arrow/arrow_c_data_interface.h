/*
 * Arrow C Data Interface
 *
 * Simplified version based on Apache Arrow C Data Interface specification.
 * https://arrow.apache.org/docs/format/CDataInterface.html
 */

#ifndef ARROW_C_DATA_INTERFACE_H
#define ARROW_C_DATA_INTERFACE_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Arrow C Data Interface structures
 */

/* Arrow schema flags */
#define ARROW_FLAG_NULLABLE 2

struct ArrowSchema {
    // Array type description
    const char* format;
    const char* name;
    const char* metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema** children;
    struct ArrowSchema* dictionary;

    // Release callback
    void (*release)(struct ArrowSchema*);
    // Opaque producer-specific data
    void* private_data;
};

struct ArrowArray {
    // Array data description
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void** buffers;
    struct ArrowArray** children;
    struct ArrowArray* dictionary;

    // Release callback
    void (*release)(struct ArrowArray*);
    // Opaque producer-specific data
    void* private_data;
};

struct ArrowArrayStream {
    // Callback to get the stream schema
    // (will be the same for all arrays in the stream).
    int (*get_schema)(struct ArrowArrayStream*, struct ArrowSchema* out);
    // Callback to get the next array
    // (if no error and the array is released, the stream has ended)
    int (*get_next)(struct ArrowArrayStream*, struct ArrowArray* out);
    // Callback to get optional detailed error information.
    const char* (*get_last_error)(struct ArrowArrayStream*);

    // Release callback
    void (*release)(struct ArrowArrayStream*);
    // Opaque producer-specific data
    void* private_data;
};

#ifdef __cplusplus
}
#endif

#endif /* ARROW_C_DATA_INTERFACE_H */
