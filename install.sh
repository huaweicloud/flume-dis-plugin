#!/bin/bash -l

plugin_name="dis-flume-plugin"
backup_suffix="dis-plugin.bak"

need_upgrade_jars=("httpclient" "httpcore" "jackson-annotations" "jackson-core" "jackson-databind")

cDir=$(cd `dirname $0`; pwd)

op="install"
if [ $# -gt 0 ]; then
    if [[ "$1" != "install" && "$1" != "uninstall" && "$1" != "-h" ]]; then
        echo "Input args '$1' is error, use 'install' as default."
    else
        op=$1
    fi
fi

cd ${cDir}
if [ ! -d "lib" ]; then
    echo "Can not find dir 'lib' in `pwd`"
    exit 1
fi

FLUME_DIR=$(dirname ${cDir})
INSTALL_TYPE=1
if [ ! -f "${FLUME_DIR}/bin/flume-ng" ]; then
    if [[ "${FLUME_DIR}" =~ plugins\.d$ ]]; then
        # from plugin.d/
        FLUME_DIR=$(dirname ${FLUME_DIR})
        INSTALL_TYPE=2
    elif [ -z "${FLUME_HOME}" ]; then
        flume_bin_path=`which flume-ng 2>/dev/null`
        if [ -z "${flume_bin_path}" ];then
            echo "Cat not find FLUME_HOME, please check environment path"
            exit 1
        else
            FLUME_DIR=$(dirname $(dirname ${flume_bin_path}))
        fi
    else
        FLUME_DIR=${FLUME_HOME}
    fi
fi

function exitOnError()
{
    if [ $1 -ne 0 ]; then
        echo "Error: $2"
        exit 1
    fi
}

function version_gt() 
{ 
    test "$(echo "$@" | tr " " "\n" | sort -V | head -n 1)" != "$1";
}

function install()
{
    cd ${FLUME_DIR}
    if [ -d "${FLUME_DIR}/plugins.d/${plugin_name}" -o -n "`ls ${FLUME_DIR}/lib/*.${backup_suffix} 2>/dev/null`" ]; then
        uninstall
    fi
    
    # Get file own and permission
    dirOwns=$(echo `stat --printf="%U:%G %a" ${FLUME_DIR}/lib` | awk '{print $1}')
    dirMods=$(echo `stat --printf="%U:%G %a" ${FLUME_DIR}/lib` | awk '{print $2}')
    fileOwns=$(echo $(stat --printf="%U:%G %a" `ls ${FLUME_DIR}/lib/* | head -1`) | awk '{print $1}')
    fileMods=$(echo $(stat --printf="%U:%G %a" `ls ${FLUME_DIR}/lib/* | head -1`) | awk '{print $2}')
    
    if [ ${INSTALL_TYPE} -eq 1 ]; then
        mkdir -p "${FLUME_DIR}/plugins.d/${plugin_name}/"
        chown ${dirOwns} ${FLUME_DIR}/plugins.d
        chmod ${dirMods} ${FLUME_DIR}/plugins.d
        cp -r ${cDir}/* ${FLUME_DIR}/plugins.d/${plugin_name}/
        exitOnError $? "can not copy lib or libext in dir '${cDir}' to ${FLUME_DIR}/plugins.d/${plugin_name}/"
        echo "-Generate [${FLUME_DIR}/plugins.d/${plugin_name}] finished."
    fi
    
    # upgrade flume jars
    cd "${FLUME_DIR}/plugins.d/${plugin_name}/libext/"
    for i in `ls *.jar`
    do
        jarName=`echo ${i} |sed -n -r 's/(.*)-[0-9.]+.*\.jar/\1/p'`
        oldJar=`find ${FLUME_DIR}/lib/ -regex "${FLUME_DIR}/lib/${jarName}-[0-9.]+.*\.jar" 2>/dev/null | head -1`
        if [ -z "${oldJar}" ]; then
            continue
        fi
        jarVersion=`echo ${i} |sed -n -r 's/.*-([0-9.]+.*)\.jar/\1/p'`
        oldJarVersion=`echo ${oldJar} |sed -n -r 's/.*-([0-9.]+.*)\.jar/\1/p'`
        if version_gt ${jarVersion} ${oldJarVersion}; then
            # begin upgrade
            mv -f "${oldJar}" "${oldJar}.${backup_suffix}"
            mv -f "${i}" "${FLUME_DIR}/lib/"
            exitOnError $? "can not copy ${FLUME_DIR}/plugins.d/${plugin_name}/libext/${i} to ${FLUME_DIR}/lib/"
            chown ${fileOwns} "${FLUME_DIR}/lib/${i}"
            chown ${fileMods} "${FLUME_DIR}/lib/${i}"
            echo "-Upgrade [${oldJar}] to [${FLUME_DIR}/lib/${i}] finished."
        else
            rm -f "${i}"
        fi
    done
    
    find ${FLUME_DIR}/plugins.d/${plugin_name} -type d | xargs chown ${dirOwns}
    find ${FLUME_DIR}/plugins.d/${plugin_name} -type d | xargs chmod ${dirMods} 
    find ${FLUME_DIR}/plugins.d/${plugin_name} -type f | xargs chown ${fileOwns}
    find ${FLUME_DIR}/plugins.d/${plugin_name} -type f | xargs chmod ${fileMods} 
    
    echo "Install ${plugin_name} successfully."
}

function uninstall()
{
    type=$1
    cd ${FLUME_DIR}

    if [ -n "`ls ${FLUME_DIR}/lib/*.${backup_suffix} 2>/dev/null`" ]; then
        for i in `ls ${FLUME_DIR}/lib/*.${backup_suffix}`
        do
            jarName=`echo ${i##*/} |sed -n -r 's/(.*)-[0-9.]+.*\.jar.*/\1/p'`
            pluginJar=`find ${FLUME_DIR}/lib/ -regex "${FLUME_DIR}/lib/${jarName}-[0-9.]+.*\.jar" 2>/dev/null | xargs ls -t | head -1`
            if [ -z "${pluginJar}" ]; then
                exitOnError 1 "can not find upgrade jar for ${i}"
                continue
            fi
            
            # delete jars
            rm -f ${pluginJar}
            mv -f ${i} ${i%.${backup_suffix}}
            
            echo "-Recovery [${i%.${backup_suffix}}] finished."
        done
    fi
    
    if [ "${type}" == "all" ]; then
        if [ -d "${FLUME_DIR}/plugins.d/${plugin_name}" -a "${type}" == "all" ]; then
            rm -rf "${FLUME_DIR}/plugins.d/${plugin_name}"
            echo "-Delete [${FLUME_DIR}/plugins.d/${plugin_name}] finished."
        fi
        echo "Uninstall ${plugin_name} successfully."
    fi
}

case ${op} in
    "uninstall")
    read -p "Are you sure to uninstall dis flume plugin?(yes|no)  :  " RESULT
    if [ "${RESULT}" == "yes" ]; then
        uninstall "all"
    else
        echo "Unknown command ${RESULT}"
    fi
    ;;
    "-h")
    echo "Usage: bash $0 [install|uninstall]"
    ;;
    *)
    install
esac