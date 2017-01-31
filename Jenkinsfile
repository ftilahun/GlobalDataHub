node{
    checkout scm
    dir('source/TransformationTesting') {
        echo 'Starting Transformation Pipeline'
        stage('Build') {
            sh 'mvn clean package'
        }
    }
}
