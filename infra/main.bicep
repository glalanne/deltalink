extension microsoftGraphV1

resource application 'Microsoft.Graph/applications@v1.0' = {
  displayName: 'DeltaLink API'
  uniqueName: 'deltalink-api'
  web:{
    redirectUris:[
      'http://localhost:8000'
    ]
  }

  requiredResourceAccess: [
    {
      resourceAppId: '00000003-0000-0000-c000-000000000000'
      resourceAccess: [
        { id: '37f7f235-527c-4136-accd-4a02d197296e', type: 'scope' }

        { id: '64a6cdd6-aab1-4aaf-94b8-3cc8405e90d0', type: 'scope' }

        { id: 'e1fe6dd8-ba31-4d61-89e7-88639da4683d', type: 'scope' }

        { id: '7427e0e9-2fba-42fe-b0c0-848c9e6a8182', type: 'scope' }
      ]
    }
  ]
}

resource resourceSp 'Microsoft.Graph/servicePrincipals@v1.0' = {
  appId: application.appId
}
