node{
    checkout scm
    dir('source/TransformationTesting') {
        echo 'Starting Transformation Pipeline'
        stage('Build') {
            withMaven {
                sh 'mvn clean test-compile'
            }
        }
        stage('Unit Test') {
            withMaven {
                sh 'mvn -Dmaven.test.skip=true package'
            }
        }
        stage('Transform') {
            echo 'Running transform on cluster'
            echo env.BRANCH_NAME
        }
        stage('Reconcile') {
            echo 'Running reconciliation on cluster'
        }
    }
}
