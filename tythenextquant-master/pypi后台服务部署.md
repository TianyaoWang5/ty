## 安装pypi
+ 直接使用pypi安装即可

```
pip install pypiserver
```
+ 然后建立存放packages的文件夹，我这块是建在用户根目录下

```
mkdir ~/packages
```

+ 后台运行，免密上传下载 
```
nohup pypi-server -P . -a . &
```
+ [加密传输配置](https://fengxc.me/pypi%E6%90%AD%E5%BB%BA%E7%A7%81%E6%9C%89%E6%9C%8D%E5%8A%A1%E5%99%A8%E5%8F%8A%E4%BD%BF%E7%94%A8.html)

# apache2.4部署
<VirtualHost *:8080>
    WSGIPassAuthorization On
    WSGIScriptAlias / /root/packages/pypiserver.wsgi
    WSGIDaemonProcess pypiserver python-path=/root/packages

    ErrorLog ${APACHE_LOG_DIR}/errorp.log
	CustomLog ${APACHE_LOG_DIR}/accessp.log combined

    <Directory /root/packages >
        WSGIProcessGroup pypiserver
        WSGIApplicationGroup %{GLOBAL}
        Require all granted
    </Directory>
</VirtualHost>


+ 创建pypiserver.wsgi文件
```python
import pypiserver
PACKAGES = '/root/packages'
HTPASSWD = '/root/htpasswd.txt'
application = pypiserver.app(root=PACKAGES, redirect_to_fallback=True, password_file=HTPASSWD)
```