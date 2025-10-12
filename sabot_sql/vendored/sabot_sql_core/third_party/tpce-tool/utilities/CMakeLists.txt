add_library(tpce_utilities STATIC
            DateTime.cpp
            EGenVersion.cpp
            Money.cpp
            Random.cpp
            condition.cpp
            error.cpp
            locking.cpp
            threading.cpp)
set(TPCE_STATIC_FILES
    ${TPCE_STATIC_FILES} $<TARGET_STATICS:tpce_utilities>
    PARENT_SCOPE)

disable_target_warnings(tpce_utilities)
