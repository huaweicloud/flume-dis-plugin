# Huawei Cloud DIS Plugins for Flume

DIS Flume Plugin是数据接入服务（DIS）为Flume开发的插件，包含DIS Source与DIS Sink。DIS Source用于从DIS服务下载数据到Flume Channel，DIS Sink用于将Flume Channel中的数据上传到DIS服务。

## 一、依赖
- Flume为1.4.0及以上版本
- Java为1.8.0及以上版本

## 二、安装插件
### 2.1 下载编译好的DIS Flume Plugin安装包，[地址](https://dis-publish.obs-website.cn-north-1.myhwclouds.com/dis-flume-plugin-1.2.0.zip)
### 2.2 使用PuTTY工具(或其他终端工具)远程登录Flume服务器
### 2.3 进入到Flume的安装目录
```console
cd ${FLUME_HOME}
```
### 2.4 上传“dis-flume-plugin-X.X.X.zip”安装包到此目录下
### 2.5 解压安装包
```console
unzip dis-flume-plugin-X.X.X.zip
```
### 2.6 进入安装包解压后的目录
```console
cd dis-flume-plugin
```
### 2.7 运行安装程序
```console
bash install.sh
```

`DIS Flume Plugin`安装在`${FLUME_HOME}/plugin.d/dis-flume-plugin`目录下，安装完成后，显示类似如下内容，表示安装成功。
```console
Install dis-flume-plugin successfully.
```

## 三、参数配置
### 3.1 [dis-flume-sink配置](https://github.com/huaweicloud/flume-dis-plugin/tree/master/dis-flume-sink)
### 3.2 [dis-flume-source配置](https://github.com/huaweicloud/flume-dis-plugin/tree/master/dis-flume-source)

## License
[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)