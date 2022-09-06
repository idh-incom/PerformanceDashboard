pipeline {
  agent {
    label 'docker'
  }

  environment {
    ProjectName = env.JOB_NAME.split('/')[0].toLowerCase().replace(".", "-") 
    ProjectImage = ""
  }

  stages {
    stage('Build') {
      steps {
        echo "Starting build of ${ProjectName}"
        script {
          ProjectImage = docker.build("${ProjectName}:${env.BUILD_ID}")
        }
      }
    }
    stage('Push') {
      when {
        branch 'master'
      }
      steps {
        script {
          docker.withRegistry('https://acrinco.azurecr.io', 'acrinco') {
            ProjectImage.push()
            ProjectImage.push('latest')
          }
        }
      }
    }
  }
  post {
    success {
      slackSend (color: '#1DED40', message: "*Build Completed* - ${env.JOB_NAME.split('/')[0]}\n*Branch:* ${env.GIT_BRANCH}; *Build:* ${env.BUILD_NUMBER}#\n${env.RUN_DISPLAY_URL}")
    }
    failure {
      slackSend (color: '#E4211B', message: "*Build Failed* - ${env.JOB_NAME.split('/')[0]}\n*Branch:* ${env.GIT_BRANCH}; *Build:* ${env.BUILD_NUMBER}#\n${env.RUN_DISPLAY_URL}")
    }
  }
}
