use crate::streaming::systems::system::System;
use crate::streaming::users::user::User;
use crate::streaming::utils::crypto;
use iggy::error::Error;
use iggy::identifier::{IdKind, Identifier};
use iggy::models::permissions::Permissions;
use iggy::models::user_status::UserStatus;
use std::sync::atomic::{AtomicU32, Ordering};
use tracing::log::error;
use tracing::{info, warn};

static USER_ID: AtomicU32 = AtomicU32::new(1);

impl System {
    pub(crate) async fn load_users(&mut self) -> Result<(), Error> {
        info!("Loading users...");
        let mut users = self.storage.user.load_all().await?;
        if users.is_empty() {
            info!("No users found, creating the root user...");
            let root = User::root();
            self.storage.user.save(&root).await?;
            info!("Created the root user.");
            users = self.storage.user.load_all().await?;
        }

        let users_count = users.len();
        let current_user_id = users.iter().map(|user| user.id).max().unwrap_or(1);
        USER_ID.store(current_user_id + 1, Ordering::SeqCst);
        self.permissioner.init(users);
        if self.config.user.authorization_enabled {
            self.permissioner.enable()
        }
        info!("Initialized {} user(s).", users_count);
        Ok(())
    }

    pub async fn create_user(
        &self,
        username: &str,
        password: &str,
        permissions: Option<Permissions>,
    ) -> Result<User, Error> {
        if self.storage.user.load_by_username(username).await.is_ok() {
            error!("User: {username} already exists.");
            return Err(Error::UserAlreadyExists);
        }
        // TODO: What if reach the max value and there are some deleted accounts (not used IDs)?
        let user_id = USER_ID.fetch_add(1, Ordering::SeqCst);
        info!("Creating user: {username} with ID: {user_id}...");
        let user = User::new(user_id, username, password, permissions);
        self.storage.user.save(&user).await?;
        info!("Created user: {username} with ID: {user_id}.");
        Ok(user)
    }

    pub async fn delete_user(&mut self, user_id: &Identifier) -> Result<User, Error> {
        let user = self.get_user(user_id).await?;
        if user.is_root() {
            error!("Cannot delete the root user.");
            return Err(Error::CannotDeleteUser(user.id));
        }

        info!("Deleting user: {} with ID: {user_id}...", user.username);
        self.storage.user.delete(&user).await?;
        self.permissioner.delete_permissions_for_user(user.id);
        info!("Deleted user: {} with ID: {user_id}.", user.username);
        Ok(user)
    }

    pub async fn update_user(
        &self,
        user_id: &Identifier,
        username: Option<String>,
        status: Option<UserStatus>,
    ) -> Result<User, Error> {
        let mut user = self.get_user(user_id).await?;
        if let Some(username) = username {
            let existing_user = self.storage.user.load_by_username(&username).await;
            if existing_user.is_ok() && existing_user.unwrap().id != user.id {
                error!("User: {username} already exists.");
                return Err(Error::UserAlreadyExists);
            }

            user.username = username;
        }

        if let Some(status) = status {
            user.status = status;
        }

        info!("Updating user: {} with ID: {user_id}...", user.username);
        self.storage.user.save(&user).await?;
        info!("Updated user: {} with ID: {user_id}.", user.username);
        Ok(user)
    }

    pub async fn update_permissions(
        &mut self,
        user_id: &Identifier,
        permissions: Option<Permissions>,
    ) -> Result<(), Error> {
        let mut user = self.get_user(user_id).await?;
        user.permissions = permissions;
        let username = user.username.clone();
        info!(
            "Updating permissions for user: {} with ID: {user_id}...",
            username
        );
        self.storage.user.save(&user).await?;
        self.permissioner.update_permissions_for_user(user);
        info!(
            "Updated permissions for user: {} with ID: {user_id}.",
            username
        );
        Ok(())
    }

    pub async fn get_user(&self, user_id: &Identifier) -> Result<User, Error> {
        Ok(match user_id.kind {
            IdKind::Numeric => {
                self.storage
                    .user
                    .load_by_id(user_id.get_u32_value()?)
                    .await?
            }
            IdKind::String => {
                self.storage
                    .user
                    .load_by_username(&user_id.get_string_value()?)
                    .await?
            }
        })
    }

    pub async fn login_user(&self, username: &str, password: &str) -> Result<User, Error> {
        info!("Logging in user: {username}...");
        let user = match self.storage.user.load_by_username(username).await {
            Ok(user) => user,
            Err(_) => {
                error!("Cannot login user: {username}.");
                return Err(Error::InvalidCredentials);
            }
        };
        if !user.is_active() {
            warn!("User: {username} is inactive.");
            return Err(Error::UserInactive);
        }
        if !crypto::verify_password(password, &user.password) {
            return Err(Error::InvalidCredentials);
        }
        // TODO: Store the currently authenticated users in memory. Keep in mind that the single user account might be used by multiple clients.
        info!("Logged in user: {username}.");
        Ok(user)
    }

    pub async fn logout_user(&self, user_id: u32) -> Result<(), Error> {
        if user_id == 0 {
            return Err(Error::InvalidCredentials);
        }
        info!("Logging out user: {user_id}...");
        // TODO: Implement user logout on the system level
        info!("Logged out user: {user_id}.");
        Ok(())
    }
}