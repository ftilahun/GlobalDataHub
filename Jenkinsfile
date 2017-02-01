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
                sh 'mvn -DskipTests=true package'
            }
        }
        stage('Transform') {
            echo 'Running transform on cluster'
            echo env.BRANCH_NAME
            def (match, source) = (env.BRANCH_NAME =~ /feature\/mapping\/(\w+)/).match[0]
            echo "Source: $source"
        }
        stage('Reconcile') {
            echo 'Running reconciliation on cluster'
        }
    }
}
