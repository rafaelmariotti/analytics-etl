## Configurations

#### Python

The Analytics team highly recommend to use [pyenv](https://github.com/pyenv/pyenv) to configure your python environment. All scripts were developed using Python version 3.4.6.

### Metadata database

The spark metadata database, used to configure all tables to be extracted, uses Postgres 9.6 to store it all.

#### Ansible

To install and test it in your machine, install ansible using:

```
brew install ansible
```
or
```
pip install ansible
```

The playbooks must be executed in the following sequence:

* emr-root-1.playbook
* emr-root-2.playbook
* emr-hadoop-1.playbook
* emr-hadoop-2.playbook

Below there are the basic commands to run ansible from your machine (remember to change your private key location):

```
cd ansible
ansible-playbook --inventory inventory/qa.hosts --user ec2-user --private-key my_key.pem --extra-vars environment_type=qa playbook/emr-root-1.playbook
ansible-playbook --inventory inventory/qa.hosts --user ec2-user --private-key my_key.pem --extra-vars environment_type=qa playbook/emr-root-2.playbook
ansible-playbook --inventory inventory/qa.hosts --user ec2-user --private-key my_key.pem --extra-vars environment_type=qa playbook/emr-hadoop-1.playbook
ansible-playbook --inventory inventory/qa.hosts --user ec2-user --private-key my_key.pem --extra-vars environment_type=qa playbook/emr-hadoop-2.playbook
```

Ansible needs to execute `git clone` and `git pull`, and for that it needs the jenkins user key for access. Therefore, you need to copy it to your local machine if you want to run it locally (try to copy the key file from Jenkins machine). All ansible configuration, including the path to jenkins ssh key file is located in `ansible/playbook/vars/variables.yml`. If you want to change the file location, just change the `git_key_path` variable.


### EMR cluster config:

1. Config files for each type of EC2 from EMR cluster can be found on ``emr-config`` folder. Please check (EMR Configuring Applications](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-configure-apps.html) documentation for more informations.

## HowTo

1. All spark scripts can be found on `scripts` folder. All scripts has a `--help` flag to assist you with all configurations;
2. All yaml config files from spark scripts can be found on `config` folder;
3. All etl SQL scripts can be found on `etl` folder;
4. All SQL files for all databases (Postgres metadata, Redshift) can be found on `sql` folder;
5. All jobs scripts to run using crontab can be found on `jobs` folder;
6. All MER files that describes our new modeling can be found on `models` folder;
7. All auxiliary scripts to execute some "brutal" tasks can be found on `aux-scripts` folder.
