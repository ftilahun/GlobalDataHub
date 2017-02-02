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
        stage('Test') {
            withMaven {
                sh 'mvn -DskipTests=true package'
            }
        }
        stage('Run Transform') {
            echo 'Running transform on cluster'
            echo env.BRANCH_NAME
            def matches = (env.BRANCH_NAME =~ /feature\/mapping\/(\w+)/)[0]
            def sourceSystem = matches[1]
            echo "Source: $sourceSystem"
            def jarFiles = findFiles(glob: 'target/*-jar-with-dependencies.jar')
            assert jarFiles.length == 1
            def jarFileName = jarFiles[0].getName()
            def jarRandomPath = '/data/jenkins/' + UUID.randomUUID().toString() + '.jar'
            echo "Copying build artifect $jarFileName to $masterNodeHost:$jarRandomPath"
            sh "scp -i $keyFile target/$jarFileName $masterNodeHost:$jarRandomPath"
            // sh "ssh -i $keyFile $masterNodeHost touch /data/jenkins/test"
        }
        stage('Run Reconciliation') {
            echo 'Running reconciliation on cluster'
        }
    }
}
