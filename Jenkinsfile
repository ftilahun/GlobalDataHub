def masterNodeHost = 'lsgnpdhmn02.nonprod.local'
def keyFile = '~/.ssh/jenkins_lsgnpdhmn'

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
            def matches = (env.BRANCH_NAME =~ /feature\/mapping\/(\w+)/)[0]
            echo "Source: $matches"
            sh "ssh -i $keyFile $masterNodeHost touch /data/jenkins/test"
        }
        stage('Reconcile') {
            echo 'Running reconciliation on cluster'
        }
    }
}
