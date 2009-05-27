# $Id$

add_subdirectory(colstore)
generate_subdir_obj_list(lucidera colstore)
add_subdirectory(bitmap)
generate_subdir_obj_list(lucidera bitmap)
add_subdirectory(libfennel_lu)
add_subdirectory(test)

if (with_farrago)
add_subdirectory(farrago)
endif()
