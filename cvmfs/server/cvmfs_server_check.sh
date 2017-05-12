#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server check" command

# This file depends on fuctions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh


cvmfs_server_check() {
  local name
  local upstream
  local storage_dir
  local url
  local check_chunks=1
  local check_integrity=0
  local subtree_path=""
  local tag=

  # optional parameter handling
  OPTIND=1
  while getopts "cit:s:" option
  do
    case $option in
      c)
        check_chunks=0
      ;;
      i)
        check_integrity=1
      ;;
      t)
        tag="-n $OPTARG"
      ;;
      s)
        subtree_path="$OPTARG"
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command check: Unrecognized option: $1"
      ;;
    esac
  done

  # get repository name
  shift $(($OPTIND-1))
  check_parameter_count_with_guessing $#
  name=$(get_or_guess_repository_name $1)

  # sanity checks
  check_repository_existence $name || die "The repository $name does not exist"

  # get repository information
  load_repo_config $name

  # more sanity checks
  is_owner_or_root $name || die "Permission denied: Repository $name is owned by $CVMFS_USER"
  health_check -r $name

  # check if repository is compatible to the installed CernVM-FS version
  check_repository_compatibility $name

  upstream=$CVMFS_UPSTREAM_STORAGE
  if is_stratum1 $name; then
    url=$CVMFS_STRATUM1
  else
    url=$CVMFS_STRATUM0
  fi

  # do it!
  if [ $check_integrity -ne 0 ]; then
    if ! is_local_upstream $upstream; then
      echo "Storage Integrity Check only works locally. skipping."
    else
      echo
      echo "Checking Storage Integrity of $name ... (may take a while)"
      storage_dir=$(get_upstream_config $upstream)
      __swissknife scrub -r ${storage_dir}/data || die "FAIL!"
    fi
  fi

  [ "x$CVMFS_LOG_LEVEL" != x ] && log_level_param="-l $CVMFS_LOG_LEVEL"
  [ $check_chunks -ne 0 ]      && check_chunks_param="-c"

  local subtree_msg=""
  local subtree_param=""
  if [ "x$subtree_path" != "x" ]; then
    subtree_param="-s '$subtree_path'"
    subtree_msg=" (starting at nested catalog '$subtree_path')"
  fi

  local with_reflog=
  has_reflog_checksum $name && with_reflog="-R $(get_reflog_checksum $name)"

  echo "Verifying integrity of ${name}${subtree_msg}..."
  local user_shell="$(get_user_shell $name)"
  local check_cmd
  check_cmd="$(__swissknife_cmd dbg) check $tag        \
                     $check_chunks_param               \
                     $log_level_param                  \
                     $subtree_param                    \
                     -r $url                           \
                     -t ${CVMFS_SPOOL_DIR}/tmp         \
                     -k ${CVMFS_PUBLIC_KEY}            \
                     -N ${CVMFS_REPOSITORY_NAME}       \
                     $(get_follow_http_redirects_flag) \
                     $with_reflog                      \
                     -z /etc/cvmfs/repositories.d/${name}/trusted_certs"
  $user_shell "$check_cmd"
}


