pipeline {
    agent any

    tools {
        // Jenkins 전역 도구 설정에 구성된 JDK 및 Maven 도구의 이름을 사용해야 합니다.
        // 예: jdk 'jdk17'
        // 예: maven 'maven_3_9_9'
    }

    stages {
        stage('Build') {
            steps {
                dir('flexible-fetch-ftp') {
                    bat 'mvn clean install'
                }
            }
        }
        stage('Archive Artifacts') {
            steps {
                archiveArtifacts artifacts: 'flexible-fetch-ftp/target/*.nar', fingerprint: true
            }
        }
    }
}
