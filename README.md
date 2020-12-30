# Cassandra client for tornado (aka tassandra)

## Запуск юнит-тестов

Тесты работают только с java 8. Для локального запуска можно временно переключиться так:

```shell
JAVA_HOME=`/usr/libexec/java_home -v 1.8`
```

Для более поздних версий возникает ошибка 
`Improperly specified VM option 'ThreadPriorityPolicy=42'. Error: Could not create the Java Virtual Machine`. 
Установка аргумента `jvm_args=['-XX:ThreadPriorityPolicy=1'])` при создании кластера не помогает.
Библиотека `ccm`, используемая в тестах для создания кластера, давно не обновляется.

## Тестирование успешного реконнекта на стенде

```shell
sudo iptables -A INPUT -p tcp --dport 9042 -j DROP  # закрываем порт, создав правило
sudo iptables -t filter -L --line-numbers -n | grep 9042  # получаем номер нужного правила (в первой колонке)
sudo iptables -t filter -D INPUT 2  # удаляем правило, 2 - это полученный номер правила
```
