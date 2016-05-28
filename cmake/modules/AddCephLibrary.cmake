include(CMakeParseArguments)

function(ceph_add_library name)
  set(kws SRCS OBJS)
  cmake_parse_arguments(AL "" "" "${kws}" ${ARGN})
  add_library(${name}-objs OBJECT ${AL_SRCS})
  if(BUILD_SHARED_LIBS)
    add_library(${name} SHARED
      $<TARGET_OBJECTS:${name}-objs>)
    if(BUILD_STATIC_LIBS)
      # in case "libfoo" is used as ${name}
      string(REGEX REPLACE "^lib" "" output_name ${name})
      add_library(${name}-static STATIC
        $<TARGET_OBJECTS:${name}-objs> ${AL_OBJS})
      set_target_properties(${name}-static PROPERTIES
        OUTPUT_NAME ${output_name})
    endif()
  else(BUILD_SHARED_LIBS)
    add_library(${name} STATIC ${name}-objs ${AL_OBJS})
  endif(BUILD_SHARED_LIBS)
endfunction(ceph_add_library)

function(ceph_link_libraries name)
  # LINK_PRIVATE instead of PRIVATE is used to backward compatibility with
  # cmake 2.8.11
  if(BUILD_SHARED_LIBS)
    target_link_libraries(${name} LINK_PRIVATE ${ARGN})
  endif()
  if(BUILD_STATIC_LIBS)
    target_link_libraries(${name}-static LINK_PRIVATE ${ARGN})
  endif()
endfunction(ceph_link_libraries)

function(ceph_install_library name)
  if(BUILD_SHARED_LIBS)
    install(TARGETS ${name} DESTINATION lib)
  endif()
  if(BUILD_STATIC_LIBS)
    install(TARGETS ${name}-static DESTINATION lib)
  endif()
endfunction()
