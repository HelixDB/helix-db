QUERY CreateUsers (name:String, email:String, password:String ) =>
    user <- AddN<User>({
        name: name,
        email: email,
        password: password,
    })
    RETURN user
 
 
 
QUERY GetUserByEmail(email: String) => 
  user <- N<User>::WHERE(_::{email}::EQ(email))
  RETURN user
 
QUERY GetAppByID(app_id: ID) => 
    app <- N<App>(app_id)
    branches <- app::Out<App_Has_Branch>
 
    RETURN app 
 
 
 
QUERY GetAppsByUserId(user_id: ID) => 
    user <- N<User>(user_id)
    apps <- user::Out<User_Has_Access_To>
    RETURN apps::{
        access_modified_at: _::InE<User_Has_Access_To>::{modified_at}::RANGE(0, 1),
        name,
        description, 
        created_at,
        favorite,
        archived,
        id,
        prod_db: _::Out<AppHasProdDb>::RANGE(0, 1)::{
            id,
            connection: _::Out<DbHasDbConnection>::RANGE(0, 1)::{
                id,
                host,
                port,
                username,
                password,
                name,
                ssl
            }
        }
    }
 
 
 
 
QUERY UpdateAppName (app_id: ID, name: String) =>
     app <- N<App>(app_id)::UPDATE({name: name})
    RETURN NONE
 
QUERY UpdateAppArchived (app_id: ID, archived: Boolean) =>
     app <- N<App>(app_id)::UPDATE({archived: archived})
    RETURN NONE
 
QUERY UpdateAppFavorite (app_id: ID, favorite_update: Boolean) =>
     app <- N<App>(app_id)::UPDATE({favorite: favorite_update})
    RETURN NONE
 
QUERY UpdateAppDescription (app_id: ID, description: String) =>
     app <- N<App>(app_id)::UPDATE({description: description})
    RETURN NONE
 
QUERY CreateFullAppWithPages (user_id: ID, app_name: String, app_description: String, created_at: Date, favorite: Boolean,archived: Boolean) =>
    user <- N<User>(user_id)
 
    // Create the main app
    app <- AddN<App>({
        name: app_name,
        description: app_description,
        created_at: created_at,
        favorite: favorite,
        archived: archived,
    })
 
    dev_branch <- AddN<Branch>({
        name: "Development"
    })
 
    prod_db <- AddN<Database>
 
app_db_edge <- AddE<AppHasProdDb>({
        created_at: created_at
    })::From(app)::To(prod_db)
 
    prod_db_connection <- AddN<DbConnection>({
        host: "TestHost",
        port: "TestPort",
        username: "TestUsername",
        password: "TestPassword",
        name: "TestName",
        ssl: "Prefer",
    })
prod_db_connection_edge <- AddE<DbHasDbConnection>({
        created_at: created_at
    })::From(prod_db)::To(prod_db_connection)
 
 
test_db <- AddN<Database>
 
    test_db_connection <- AddN<DbConnection>({
host: "TestHost",
   port: "TestPort",
   username: "TestUsername",
   password: "TestPassword",
   name: "TestName",
   ssl: "Prefer",
    })
 
 
test_db_connection_edge <- AddE<DbHasDbConnection>({
        created_at: created_at
    })::From(test_db)::To(test_db_connection)
 
 
    staging_branch <- AddN<Branch>({
        name: "Staging"
    })
 
   test_db_stage_branch_edge <- AddE<BranchHasDbConnection>({
        created_at: created_at
    })::From(staging_branch)::To(test_db)
 
test_db_dev_branch_edge <- AddE<BranchHasDbConnection>({
        created_at: created_at
    })::From(dev_branch)::To(test_db)
 
 
    frontend_dev <- AddN<Frontend>
    backend_dev <- AddN<Backend>
 
    frontend_staging <- AddN<Frontend>
    backend_staging <- AddN<Backend>
 
    root_element <- AddN<Element>({
        element_id: "root_element",
        name: "root_element"
    })
 
    root_element_404 <- AddN<Element>({
        element_id: "root_element",
        name: "root_element"
    })
 
    root_element_reset <- AddN<Element>({
        element_id: "root_element", 
        name: "root_element"
    })
 
    index_page <- AddN<Page>({
        name: "index"
    })
 
    not_found_page <- AddN<Page>({
        name: "Page not found"
    })
 
    reset_password_page <- AddN<Page>({
        name: "Reset Password"
    })
 
    main_folder <- AddN<PageFolder>({
        name: "Unsorted"
    })
    backend_dev_root_folder <- AddN<FolderTree>({
        name: "API",
        description: "Main folder where your endpoints and functions exist."
    })
    backend_staging_root_folder <- AddN<FolderTree>({
        name: "API",
        description: "Main folder where your endpoints and functions exist."
    })
    backend_dev_root_folder_edge <- AddE<Backend_Has_Root_Folder>({
        created_at: created_at,
    })::From(backend_dev)::To(backend_dev_root_folder)
backend_staging_root_folder_edge <- AddE<Backend_Has_Root_Folder>({
        created_at: created_at,
    })::From(backend_staging)::To(backend_staging_root_folder)
 
    user_app_edge <- AddE<User_Has_Access_To>({
        created_at: created_at,
        modified_at: created_at
    })::From(user)::To(app)
 
    app_dev_branch_edge <- AddE<App_Has_Branch>({
        created_at: created_at
    })::From(app)::To(dev_branch)
 
    app_staging_branch_edge <- AddE<App_Has_Branch>({
        created_at: created_at
    })::From(app)::To(staging_branch)
 
    dev_branch_frontend_edge <- AddE<Branch_Has_Frontend>({
        created_at: created_at
    })::From(dev_branch)::To(frontend_dev)
 
    dev_branch_backend_edge <- AddE<Branch_Has_Backend>({
        created_at: created_at
    })::From(dev_branch)::To(backend_dev)
 
    staging_branch_frontend_edge <- AddE<Branch_Has_Frontend>({
        created_at: created_at
    })::From(staging_branch)::To(frontend_staging)
 
    staging_branch_backend_edge <- AddE<Branch_Has_Backend>({
        created_at: created_at
    })::From(staging_branch)::To(backend_staging)
 
    index_page_element_edge <- AddE<Page_Has_Root_Element>({
        assigned_at: created_at
    })::From(index_page)::To(root_element)
 
    not_found_page_element_edge <- AddE<Page_Has_Root_Element>({
        assigned_at: created_at
    })::From(not_found_page)::To(root_element_404)
 
    reset_page_element_edge <- AddE<Page_Has_Root_Element>({
        assigned_at: created_at
    })::From(reset_password_page)::To(root_element_reset)
 
    folder_index_edge <- AddE<PageFolder_Contains_Page>({
        assigned_at: created_at
    })::From(main_folder)::To(index_page)
 
    folder_404_edge <- AddE<PageFolder_Contains_Page>({
        assigned_at: created_at
    })::From(main_folder)::To(not_found_page)
 
    folder_reset_edge <- AddE<PageFolder_Contains_Page>({
        assigned_at: created_at
    })::From(main_folder)::To(reset_password_page)
 
    frontend_index_edge <- AddE<Frontend_Has_Page>({
        assigned_at: created_at
    })::From(frontend_dev)::To(index_page)
 
    frontend_404_edge <- AddE<Frontend_Has_Page>({
        assigned_at: created_at
    })::From(frontend_dev)::To(not_found_page)
 
    frontend_reset_edge <- AddE<Frontend_Has_Page>({
        assigned_at: created_at
    })::From(frontend_dev)::To(reset_password_page)
 
    frontend_folder_edge <- AddE<Frontend_Contains_PageFolder>({
        assigned_at: created_at
    })::From(frontend_dev)::To(main_folder)
 
 
 
RETURN { 
    app: {
        branches: [
            {
                name: dev_branch::{name},
                frontend:  {
                    page_folders: [
                        {
                            name: main_folder::{name},
                            pages: [index_page, not_found_page, reset_password_page]
                        }
                    ],
                    id:frontend_dev::{id}
                },
                test_db: {
                    id: test_db::{id},
                    connection: {
                        id: test_db_connection::{id},
                        host: test_db_connection::{host},
                        port: test_db_connection::{port},
                        username: test_db_connection::{username},
                        password:test_db_connection::{password},
                        name: test_db_connection::{name},
                        ssl:test_db_connection::{ssl}
                      },
                  },
                backend: {
                    id: backend_dev::{id},
                    root_folder: {
                        id:backend_dev_root_folder::{id},
                        name: backend_dev_root_folder::{name},
                        description: backend_dev_root_folder::{description}
                      },
                  }
            },
            {
                name: staging_branch::{name},
frontend:  {
                    page_folders: [
                        {
                            name: main_folder::{name},
                            pages: [index_page, not_found_page, reset_password_page]
                        }
                    ],
                    id:frontend_dev::{id}
                },test_db: {
                    id: test_db::{id},
                    connection: {
                        id: test_db_connection::{id},
                        host: test_db_connection::{host},
                        port: test_db_connection::{port},
                        username: test_db_connection::{username},
                        password:test_db_connection::{password},
                        name: test_db_connection::{name},
                        ssl:test_db_connection::{ssl}
                      },
                  },
                backend: {
                    id: backend_staging::{id},
                    root_folder: {
                        id:backend_staging_root_folder::{id},
                        name: backend_staging_root_folder::{name},
                        description: backend_staging_root_folder::{description}
                      },
                  }
            }
        ],
        name: app::{name},
        description: app::{description},
        favorite: app::{favorite},
        archived: app::{archived},
        id:app::{id},
prod_db: {
                    id: prod_db::{id},
                    connection: {
                        id: prod_db_connection::{id},
                        host: prod_db_connection::{host},
                        port: prod_db_connection::{port},
                        username: prod_db_connection::{username},
                        password:prod_db_connection::{password},
                        name: prod_db_connection::{name},
                        ssl:prod_db_connection::{ssl}
                      }
                  }
    }
}
 