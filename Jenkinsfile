startMessage()

pipeline {
    agent any

    tools{
    }

    parameters {
        choice(name: 'operation', choices: 'deploy\nrollback', description: 'choose if you want to deploy or rollback your project')
        string(name: 'version', description: 'Project version to deploy')
    }
    
    stages {
        stage('Build') {
            steps {
                sh "mvn clean install -Dmaven.test.skip=true -f $WORKSPACE/pom.xml"
                
                sh "sudo /usr/local/bin/docker-compose --project-name spark -f $WORKSPACE/docker-compose.prod.yml build --force-rm --no-cache --pull worker"
                sh "sudo docker tag spark_worker:latest rafaelmariotti/spark-worker-deleted-queue:${params.version}"
                sh "sudo docker push rafaelmariotti/spark-worker-deleted-queue:${params.version}"
                
                sh "sudo docker rmi -f spark_worker:latest"
                sh "sudo docker rmi -f rafaelmariotti/spark-worker-deleted-queue:${params.version}"
            }

            post {
                failure {
                    failMessage()
                }

            }

        }

        stage('Deploy') {
            steps{
                sh "sudo /usr/local/bin/ansible-playbook --inventory 'localhost,' --user ec2-user --private-key /var/.ssh/my_key.pem --extra-vars version=${params.version} --extra-vars environment_type=prod $WORKSPACE/ansible/spark-worker-prepare.playbook"

                sh "sudo /usr/local/bin/ansible-playbook --inventory $WORKSPACE/ansible/inventory-prod.hosts --user ec2-user --private-key /var/.ssh/my_key.pem --extra-vars workspace=$WORKSPACE --extra-vars version=${params.version} --extra-vars environment_type=prod $WORKSPACE/ansible/spark-worker-deploy.playbook"
            }

            post {
                success {
                    finishedMessage()
                }
                failure {
                    failMessage()
                }
            }

        }
    }
}

def startMessage() {
    slackSend (channel: '#analytics_jenkins', color: '#00FF00', message: "[INFO] Starting deployment of spark-worker version '${params.version}' in PROD environment")
}

def finishedMessage() {
    slackSend (channel: '#analytics_jenkins', color: '#00FF00', message: "[INFO] Finished deployment of spark-worker version ${params.version} in PROD environment")
}

def failMessage() {
    slackSend (channel: '#analytics_jenkins', color: '#FF0000', message: "[ERROR] Deployment fail from spark-worker version ${params.version} in PROD environment")
}
