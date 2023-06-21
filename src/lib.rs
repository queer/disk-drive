use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use eyre::Result;
use floppy_disk::prelude::*;
use tokio::io::AsyncWriteExt;
use tracing::{error, trace, warn};

pub struct DiskDrive<
    'a,
    'b,
    F1: FloppyDisk<'a> + FloppyDiskUnixExt + Send + Sync + 'a,
    F2: FloppyDisk<'b> + FloppyDiskUnixExt + Send + Sync + 'b,
> where
    <F1 as FloppyDisk<'a>>::Permissions: FloppyUnixPermissions,
    <F1 as FloppyDisk<'a>>::Metadata: FloppyUnixMetadata,

    <F1 as FloppyDisk<'a>>::DirBuilder: Send,
    <F1 as FloppyDisk<'a>>::DirEntry: Send,
    <F1 as FloppyDisk<'a>>::File: Send,
    <F1 as FloppyDisk<'a>>::FileType: Send,
    <F1 as FloppyDisk<'a>>::Metadata: Send,
    <F1 as FloppyDisk<'a>>::OpenOptions: Send,
    <F1 as FloppyDisk<'a>>::Permissions: Send,
    <F1 as FloppyDisk<'a>>::ReadDir: Send,

    <F2 as FloppyDisk<'b>>::Permissions: FloppyUnixPermissions,
    <F2 as FloppyDisk<'b>>::Metadata: FloppyUnixMetadata,

    <F2 as FloppyDisk<'b>>::DirBuilder: Send,
    <F2 as FloppyDisk<'b>>::DirEntry: Send,
    <F2 as FloppyDisk<'b>>::File: Send,
    <F2 as FloppyDisk<'b>>::FileType: Send,
    <F2 as FloppyDisk<'b>>::Metadata: Send,
    <F2 as FloppyDisk<'b>>::OpenOptions: Send,
    <F2 as FloppyDisk<'b>>::Permissions: Send,
    <F2 as FloppyDisk<'b>>::ReadDir: Send,
{
    _f1: std::marker::PhantomData<&'a F1>,
    _f2: std::marker::PhantomData<&'b F2>,
}

impl<
        'a,
        'b,
        F1: FloppyDisk<'a> + FloppyDiskUnixExt + Send + Sync + 'a,
        F2: FloppyDisk<'b> + FloppyDiskUnixExt + Send + Sync + 'b,
    > DiskDrive<'a, 'b, F1, F2>
where
    <F1 as FloppyDisk<'a>>::Permissions: FloppyUnixPermissions,
    <F1 as FloppyDisk<'a>>::Metadata: FloppyUnixMetadata,
    <F2 as FloppyDisk<'b>>::Permissions: FloppyUnixPermissions,
    <F2 as FloppyDisk<'b>>::Metadata: FloppyUnixMetadata,
{
    pub async fn copy_between(src: &'a F1, dest: &'b F2) -> Result<()> {
        Self::do_copy(src, dest, None, None).await
    }

    pub async fn copy_from_src<P: Into<PathBuf>>(
        src: &'a F1,
        dest: &'b F2,
        src_scope: P,
    ) -> Result<()> {
        let src_scope = src_scope.into();
        let src_scope = if !src_scope.starts_with("/") {
            PathBuf::from("/").join(src_scope)
        } else {
            src_scope
        };
        Self::do_copy(src, dest, Some(src_scope), None).await
    }

    pub async fn copy_to_dest<P: Into<PathBuf>>(
        src: &'a F1,
        dest: &'b F2,
        dest_scope: P,
    ) -> Result<()> {
        let dest_scope = dest_scope.into();
        let dest_scope = if !dest_scope.starts_with("/") {
            PathBuf::from("/").join(dest_scope)
        } else {
            dest_scope
        };
        Self::do_copy(src, dest, None, Some(dest_scope)).await
    }

    pub async fn copy_from_src_to_dest<P: Into<PathBuf>, Q: Into<PathBuf>>(
        src: &'a F1,
        dest: &'b F2,
        src_scope: P,
        dest_scope: Q,
    ) -> Result<()> {
        let src_scope = src_scope.into();
        let dest_scope = dest_scope.into();
        let src_scope = if !src_scope.starts_with("/") {
            PathBuf::from("/").join(src_scope)
        } else {
            src_scope
        };
        let dest_scope = if !dest_scope.starts_with("/") {
            PathBuf::from("/").join(dest_scope)
        } else {
            dest_scope
        };
        Self::do_copy(src, dest, Some(src_scope), Some(dest_scope)).await
    }

    async fn do_copy(
        src: &'a F1,
        dest: &'b F2,
        src_path: Option<PathBuf>,
        dest_path: Option<PathBuf>,
    ) -> Result<()> {
        let src_path = src_path.unwrap_or_else(|| PathBuf::from("/"));
        let dest_path = dest_path.unwrap_or_else(|| PathBuf::from("/"));
        let paths = if src.metadata(&src_path).await?.is_file() {
            trace!("copying src file {}", src_path.display());
            let mut out = BTreeSet::new();
            out.insert(src_path);
            out
        } else {
            trace!("copying src dir {}", src_path.display());
            nyoom::walk_ordered(src, src_path).await?
        };
        for src_path in paths {
            trace!("processing src_path: {}", src_path.display());
            match <F1 as FloppyDisk<'a>>::read_link(src, &src_path).await {
                Ok(_) => {
                    trace!(
                        "copy symlink {} -> {}",
                        src_path.display(),
                        dest_path.display()
                    );
                    Self::add_symlink_to_memfs(src, dest, &src_path, &dest_path).await?;
                }
                Err(_) => {
                    let metadata = <F1 as FloppyDisk<'a>>::metadata(src, &src_path).await?;
                    let file_type = metadata.file_type();
                    if file_type.is_dir() {
                        trace!("copy dir {} -> {}", src_path.display(), dest_path.display());
                        Self::copy_dir_to_memfs(src, dest, &src_path, &dest_path).await?;
                    } else if file_type.is_file() {
                        trace!(
                            "copy file {} -> {}",
                            src_path.display(),
                            dest_path.display()
                        );
                        Self::copy_file_to_memfs(src, dest, &src_path, &dest_path).await?;
                    } else {
                        error!("unknown file type for source path {src_path:?}");
                    }
                }
            };
        }

        Ok(())
    }

    async fn copy_file_to_memfs(
        src: &'a F1,
        dest: &'b F2,
        src_path: &Path,
        dest_path: &Path,
    ) -> Result<()> {
        let dest_path = dest_path.join(src_path);
        let dest_path = dest_path.as_path();
        trace!("creating file {dest_path:?}");
        if let Some(memfs_parent) = dest_path.parent() {
            dest.create_dir_all(memfs_parent).await?;
        }

        let mut src_handle: <F1 as FloppyDisk>::File = <F1::OpenOptions>::new()
            .read(true)
            .open(src, src_path)
            .await?;
        {
            if let Some(parent) = dest_path.parent() {
                trace!("creating dest file parents");
                dest.create_dir_all(parent).await?;
            }

            let dest_metadata = <F2 as FloppyDisk>::metadata(dest, dest_path).await;
            // if dest doesn't exist, just copy directly
            if dest_metadata.is_err() {
                trace!("dest file {dest_path:?} doesn't exist, copying directly!");
                let mut dest_handle: <F2 as FloppyDisk>::File = <F2::OpenOptions>::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(dest, dest_path)
                    .await?;
                tokio::io::copy(&mut src_handle, &mut dest_handle).await?;

                // copy permissions
                let src_metadata = src_handle.metadata().await?;
                let src_permissions = src_metadata.permissions();
                let mode = <<F1 as FloppyDisk<'_>>::Permissions as FloppyUnixPermissions>::mode(
                    &src_permissions,
                );

                let permissions = <<F2 as FloppyDisk>::Permissions>::from_mode(mode);
                let uid = src_metadata.uid()?;
                let gid = src_metadata.gid()?;

                <F2 as FloppyDiskUnixExt>::chown(dest, dest_path, uid, gid).await?;
                <F2 as FloppyDisk>::set_permissions(dest, dest_path, permissions).await?;

                return Ok(());
            }

            let mut dest_handle: <F2 as FloppyDisk>::File = <F2::OpenOptions>::new()
                .read(true)
                .write(true)
                .open(dest, dest_path)
                .await?;

            // if dest exists and is a dir, copy into it
            let dest_metadata = dest_metadata?;
            if dest_metadata.is_dir() {
                trace!("copying into dir {dest_path:?}");
                let dest_path = dest_path.join(Path::new(src_path.file_name().unwrap()));
                trace!("target path = {dest_path:?}");
                let written = tokio::io::copy(&mut src_handle, &mut dest_handle).await?;
                trace!("wrote {written} bytes");
                dest_handle.flush().await?;

                // copy permissions
                let src_metadata = src_handle.metadata().await?;
                let src_permissions = src_metadata.permissions();
                let mode = <<F1 as FloppyDisk<'_>>::Permissions as FloppyUnixPermissions>::mode(
                    &src_permissions,
                );

                let permissions = <<F2 as FloppyDisk>::Permissions>::from_mode(mode);
                let uid = src_metadata.uid()?;
                let gid = src_metadata.gid()?;

                <F2 as FloppyDiskUnixExt>::chown(dest, &dest_path, uid, gid).await?;
                <F2 as FloppyDisk>::set_permissions(dest, &dest_path, permissions).await?;

                return Ok(());
            }

            // if dest exists and is a file, copy into it
            if dest_metadata.is_file() {
                trace!("overwriting dest file {dest_path:?}");
                tokio::io::copy(&mut src_handle, &mut dest_handle).await?;

                // copy permissions
                let src_metadata = src_handle.metadata().await?;
                let src_permissions = src_metadata.permissions();
                let mode = <<F1 as FloppyDisk<'_>>::Permissions as FloppyUnixPermissions>::mode(
                    &src_permissions,
                );

                let permissions = <<F2 as FloppyDisk>::Permissions>::from_mode(mode);
                let uid = src_metadata.uid()?;
                let gid = src_metadata.gid()?;

                <F2 as FloppyDiskUnixExt>::chown(dest, dest_path, uid, gid).await?;
                <F2 as FloppyDisk>::set_permissions(dest, dest_path, permissions).await?;

                return Ok(());
            }

            // if dest exists and is a symlink, log error and return
            if dest_metadata.is_symlink() {
                warn!("dest file path {dest_path:?} is a symlink, skipping copy!");
                return Ok(());
            }
        }

        let src_metadata = src_handle.metadata().await?;
        let src_permissions = src_metadata.permissions();
        let mode =
            <<F1 as FloppyDisk<'_>>::Permissions as FloppyUnixPermissions>::mode(&src_permissions);
        let permissions = <<F2 as FloppyDisk>::Permissions>::from_mode(mode);
        let uid = src_metadata.uid()?;
        let gid = src_metadata.gid()?;
        <F2 as FloppyDiskUnixExt>::chown(dest, dest_path, uid, gid).await?;
        <F2 as FloppyDisk>::set_permissions(dest, dest_path, permissions).await?;

        Ok(())
    }

    async fn copy_dir_to_memfs(
        src: &'a F1,
        dest: &'b F2,
        src_path: &Path,
        dest_path: &Path,
    ) -> Result<()> {
        let dest_path = dest_path.join(src_path);
        let dest_path = dest_path.as_path();
        trace!("creating dir {dest_path:?}");
        dest.create_dir_all(dest_path).await?;

        let src_metadata = src.metadata(src_path).await?;
        let mode = src_metadata.permissions().mode();
        let permissions = <F2 as FloppyDisk>::Permissions::from_mode(mode);
        dest.set_permissions(dest_path, permissions).await?;
        dest.chown(dest_path, src_metadata.uid()?, src_metadata.gid()?)
            .await?;

        Ok(())
    }

    async fn add_symlink_to_memfs(
        src: &F1,
        dest: &F2,
        src_path: &Path,
        dest_path: &Path,
    ) -> Result<()> {
        let dest_path = dest_path.join(src_path);
        let dest_path = dest_path.as_path();
        let link = src.read_link(src_path).await?;
        trace!("linking {dest_path:?} to {link:?}");
        dest.symlink(link, dest_path.into()).await?;

        Ok(())
    }
}
